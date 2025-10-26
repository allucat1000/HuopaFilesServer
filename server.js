// deno-lint-ignore-file no-import-prefix no-unversioned-import
import express from "npm:express";
import rateLimit from "npm:express-rate-limit";
import { createClient } from "https://esm.sh/@supabase/supabase-js";
import { Buffer } from "node:buffer";

const supabase = createClient(
  Deno.env.get("SUPABASE_URL"),
  Deno.env.get("SUPABASE_KEY")
);

const PROJECT_NAME = "files"

const auth = Deno.env.get("AUTH");
const kv = await Deno.openKv();

const app = express();

app.use((req, _res, next) => {
  if (req.method === "POST" && req.headers["type"] === "file") {
    const chunks = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("end", () => {
      req.body = Buffer.concat(chunks);
      next();
    });
  } else {
    next();
  }
});

app.use(express.text({ limit: "10mb" }));
app.use(express.json({ limit: "10mb" }));

const limiter = rateLimit({
  windowMs: 60 * 1000,
  max: 100,
  standardHeaders: "draft-8",
  legacyHeaders: false,
});

const writeCache = new Map();
const MAX_CACHE_SIZE = 500 * 1024;

app.use(limiter);

app.use((req, res, next) => {
  if (!auth) return res.sendStatus(500);
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Content-Type, type");
  res.header("Access-Control-Allow-Methods", "DELETE")
  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");
  res.setHeader("Surrogate-Control", "no-store");
  if (req.method === "OPTIONS") return res.sendStatus(200);
  next();
});

function checkAuth(req) {
  return req.query.auth === auth;
}

function getContentSize(content) {
  if (typeof content === "string") return new TextEncoder().encode(content).length;
  if (content instanceof ArrayBuffer || ArrayBuffer.isView(content)) return content.byteLength;
  if (content instanceof Blob) return content.size;
  return new TextEncoder().encode(JSON.stringify(content)).length;
}

async function authHandler(req, res, method) {
  const path = req.path;
  if (method === "GET" && path === "/authCheck") return res.sendStatus(checkAuth(req) ? 200 : 403);
  if (!checkAuth(req)) return res.sendStatus(403);

  if (method === "GET") return await fileGet(req, res);
  if (method === "POST") return await filePost(req, res);

  return res.sendStatus(405);
}

async function fileGet(req, res) {
  const pathKey = req.path === "/" ? "root.json" : req.path.startsWith("/") ? req.path.slice(1) : req.path;

  if (writeCache.has(req.path)) {
    const cached = writeCache.get(req.path);

    const content = cached.meta.type === "dir"
      ? JSON.parse(cached.content)
      : Buffer.from(cached.content).toString("base64");

    return res.json({
      filename: cached.meta.filename,
      size: cached.meta.size,
      content,
      modified: cached.meta.modified,
      created: cached.meta.created,
      type: cached.meta.type,
      contenttype: cached.meta.contenttype,
      cached: true,
    });
  }

  const metaResult = await kv.get([req.path]);
  const meta = metaResult?.value;
  if (!meta) return res.sendStatus(404);

  const { data, error } = await supabase.storage.from(PROJECT_NAME).download(pathKey);
  if (error || !data) return res.sendStatus(404);

  if (meta.type === "dir") {
    const text = await data.text();
    return res.json({
      filename: meta.filename,
      size: meta.size,
      content: JSON.parse(text),
      modified: meta.modified,
      created: meta.created,
      type: meta.type,
      contenttype: meta.contenttype,
    });
  } else {
    const arrayBuffer = await data.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);
    return res.json({
      filename: meta.filename,
      size: meta.size,
      content: buffer.toString("base64"),
      modified: meta.modified,
      created: meta.created,
      type: meta.type,
      contenttype: meta.contenttype,
    });
  }
}

async function filePost(req, res) {
  const path = req.path;
  const type = req.headers["type"] === "dir" ? "dir" : "file";
  const currentData = await kv.get([path]);
  const createdAt = currentData?.value?.created ?? Date.now();
  const modifiedAt = Date.now();
  const pathKey = path === "/" ? "root.json" : path.slice(1);

  let fileContent;
  let contentType;

  if (type === "dir") {
    fileContent = JSON.stringify([]);
    contentType = "application/json";
  } else {
    if (Buffer.isBuffer(req.body)) {
      fileContent = req.body;
      contentType = req.headers["content-type"] || "application/octet-stream";
    } else if (typeof req.body === "string") {
      fileContent = req.body;
      contentType = req.headers["content-type"] || "text/plain";
    } else {
      fileContent = JSON.stringify(req.body);
      contentType = "application/json";
    }
  }

  const size = getContentSize(fileContent);

  // Cache the uploaded file
  if (size <= MAX_CACHE_SIZE) {
    writeCache.set(req.path, {
      content: fileContent,
      meta: {
        filename: path,
        size,
        modified: modifiedAt,
        created: createdAt,
        type,
        contenttype: contentType,
      },
      timestamp: Date.now(),
    });
    res.sendStatus(200);
  } else {
    res.sendStatus(202);
  }

  (async () => {
    const { error: uploadError } = await supabase.storage.from(PROJECT_NAME).upload(
      pathKey,
      Buffer.isBuffer(fileContent) ? fileContent : Buffer.from(fileContent),
      { contentType, upsert: true }
    );

    if (!uploadError) {
      await kv.set([path], {
        filename: path,
        size,
        modified: modifiedAt,
        created: createdAt,
        type,
        contenttype: contentType,
      });

      // Update parent directory
      const lastSlash = path.lastIndexOf("/");
      const parentPath = lastSlash > 0 ? path.slice(0, lastSlash) : "/";
      if (parentPath !== path) {
        const parentKey = parentPath === "/" ? "root.json" : parentPath.slice(1);
        let parentContent = [];

        // Use cache first
        if (writeCache.has(parentPath)) {
          const cached = writeCache.get(parentPath);
          parentContent = cached.meta.type === "dir" ? JSON.parse(cached.content) : [];
        } else {
          try {
            const { data, error } = await supabase.storage.from(PROJECT_NAME).download(parentKey);
            if (!error && data) {
              const text = await data.text();
              parentContent = JSON.parse(text) || [];
            }
          } catch {/**/}
        }

        const existingIndex = parentContent.findIndex(e => e.filename === path);
        const entry = { filename: path, modified: modifiedAt, created: createdAt, size, type };
        if (existingIndex >= 0) parentContent[existingIndex] = entry;
        else parentContent.push(entry);

        const jsonContent = JSON.stringify(parentContent);

        await supabase.storage.from(PROJECT_NAME).upload(
          parentKey,
          Buffer.from(jsonContent),
          { contentType: "application/json", upsert: true }
        );

        // Cache the updated parent directory
        const parentSize = getContentSize(jsonContent);
        if (parentSize <= MAX_CACHE_SIZE) {
          writeCache.set(parentPath, {
            content: jsonContent,
            meta: {
              filename: parentPath,
              size: parentSize,
              modified: Date.now(),
              created: Date.now(),
              type: "dir",
              contenttype: "application/json",
            },
            timestamp: Date.now(),
          });
        }

      }

      setTimeout(() => writeCache.delete(req.path), 30000);
    } else {
      console.error("Upload error:", uploadError);
    }
  })();
}



async function fileDelete(req, res) {
  const path = req.path;

  const metaResult = await kv.get([path]);
  const meta = metaResult?.value;
  if (!meta) return res.sendStatus(404);

  const visited = new Set();

  async function deleteRecursively(targetPath) {
    if (visited.has(targetPath)) return;
    visited.add(targetPath);

    const targetKey = targetPath === "/" ? "root.json" : targetPath.slice(1);

    const metaResult = await kv.get([targetPath]);
    const meta = metaResult?.value;
    if (!meta) return;

    if (meta.type === "dir") {
      try {
        const { data, error } = await supabase.storage.from(PROJECT_NAME).download(targetKey);
        if (!error && data) {
          const text = await data.text();
          const children = JSON.parse(text) || [];
          await Promise.all(children.map(child => deleteRecursively(child.filename)));
        }
      } catch (err) {
        console.error(`Failed to read directory before deletion: ${targetPath}`, err);
      }
    }

    const { error: deleteError } = await supabase.storage.from(PROJECT_NAME).remove([targetKey]);
    if (deleteError) console.error("Supabase delete error:", deleteError);

    await kv.delete([targetPath]);
    writeCache.delete(targetPath);
  }

  await deleteRecursively(path);

  // Update parent directory content
  const lastSlash = path.lastIndexOf("/");
  const parentPath = lastSlash > 0 ? path.slice(0, lastSlash) : "/";
  if (parentPath !== path) {
    const parentKey = parentPath === "/" ? "root.json" : parentPath.slice(1);
    let parentContent = [];

    try {
      const { data, error } = await supabase.storage.from(PROJECT_NAME).download(parentKey);
      if (!error && data) {
        const text = await data.text();
        parentContent = JSON.parse(text) || [];
      }
    } catch {/**/}

    // Remove deleted file/dir entry
    parentContent = parentContent.filter(e => e.filename !== path);

    const jsonContent = JSON.stringify(parentContent);

    // Upload updated parent directory list
    await supabase.storage.from(PROJECT_NAME).upload(
      parentKey,
      Buffer.from(jsonContent),
      { contentType: "application/json", upsert: true }
    );

    // Cache updated parent directory properly as JSON text
    const parentSize = getContentSize(jsonContent);
    if (parentSize <= MAX_CACHE_SIZE) {
      writeCache.set(parentPath, {
        content: jsonContent,
        meta: {
          filename: parentPath,
          size: parentSize,
          modified: Date.now(),
          created: Date.now(),
          type: "dir",
          contenttype: "application/json",
        },
        timestamp: Date.now(),
      });
    }
  }

  res.sendStatus(200);
}





app.get(/.*/, (req, res) => authHandler(req, res, "GET"));
app.post(/.*/, (req, res) => authHandler(req, res, "POST"));
app.delete(/.*/, (req, res) => {
  if (!checkAuth(req)) return res.sendStatus(403);
  return fileDelete(req, res);
});
app.listen(3000);
