require("dotenv").config();

const path = require("path");
const fs = require("fs");
const express = require("express");
const session = require("express-session");
const bcrypt = require("bcrypt");
const multer = require("multer");
const { parse } = require("csv-parse");
const XLSX = require("xlsx");
const mongoose = require("mongoose");
const { v2: cloudinary } = require("cloudinary");
const crypto = require("crypto");

const app = express();

const PORT = Number(process.env.PORT || 3001);
const SESSION_SECRET = process.env.SESSION_SECRET || "change-me";
const MONGODB_URI = process.env.MONGODB_URI || "";
const USE_MONGO = Boolean(MONGODB_URI);
const CLOUDINARY_URL = process.env.CLOUDINARY_URL || "";
const CLOUDINARY_CLOUD_NAME = process.env.CLOUDINARY_CLOUD_NAME || "";
const CLOUDINARY_API_KEY = process.env.CLOUDINARY_API_KEY || "";
const CLOUDINARY_API_SECRET = process.env.CLOUDINARY_API_SECRET || "";
const CLOUDINARY_FOLDER = process.env.CLOUDINARY_FOLDER || "kanshasai-prizes";
const USE_CLOUDINARY = Boolean(CLOUDINARY_URL || (CLOUDINARY_CLOUD_NAME && CLOUDINARY_API_KEY && CLOUDINARY_API_SECRET));
const CLOUDINARY_DEDUP = (process.env.CLOUDINARY_DEDUP || "1") === "1";

const DATA_DIR = path.join(__dirname, "data");
const UPLOADS_DIR = path.join(__dirname, "uploads");
const IMAGE_DIR = path.join(__dirname, "image");
const PRIZES_JSON = path.join(DATA_DIR, "prizes.json");
const USERS_JSON = path.join(DATA_DIR, "users.json");
// Default image shown for imported prizes (can override in .env)
const DEFAULT_IMAGE_PATH = process.env.DEFAULT_IMAGE_PATH || "/image/IMG_0428.jpg";

function ensureDirs() {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.mkdirSync(UPLOADS_DIR, { recursive: true });
}

function readJson(filePath, fallback) {
  try {
    const raw = fs.readFileSync(filePath, "utf8");
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}

function writeJsonAtomic(filePath, data) {
  const tmp = `${filePath}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(data, null, 2), "utf8");
  fs.renameSync(tmp, filePath);
}

function nowIso() {
  return new Date().toISOString();
}

let PrizeModel = null;
let UserModel = null;

function defineModels() {
  if (PrizeModel && UserModel) return;

  const prizeSchema = new mongoose.Schema(
    {
      id: { type: String, required: true, unique: true, index: true },
      title: { type: String, required: true },
      description: { type: String, default: "" },
      category: { type: String, default: "" },
      setName: { type: String, default: "" },
      tags: { type: [String], default: [] },
      priceYen: { type: Number, default: 0 },
      quantity: { type: Number, default: 0 },
      imagePath: { type: String, default: "" },
      imagePublicId: { type: String, default: "" },
      detailImagePath: { type: String, default: "" },
      detailImagePublicId: { type: String, default: "" },
      createdAt: { type: String, default: "" },
      updatedAt: { type: String, default: "" },
    },
    { versionKey: false }
  );

  const userSchema = new mongoose.Schema(
    {
      id: { type: String, required: true, unique: true, index: true },
      role: { type: String, required: true, index: true },
      username: { type: String, default: "", index: true },
      email: { type: String, default: "", index: true },
      passwordHash: { type: String, required: true },
      createdAt: { type: String, default: "" },
    },
    { versionKey: false }
  );

  PrizeModel = mongoose.models.Prize || mongoose.model("Prize", prizeSchema);
  UserModel = mongoose.models.User || mongoose.model("User", userSchema);
}

async function initDbIfNeeded() {
  if (!USE_MONGO) return;
  if (mongoose.connection.readyState === 1) return;
  await mongoose.connect(MONGODB_URI);
  defineModels();
  console.log("✅ MongoDBに接続しました");
}

async function seedAdminIfNeeded() {
  const username = process.env.ADMIN_ID || "admin";
  const email = process.env.ADMIN_EMAIL || "admin@example.com";
  const password = process.env.ADMIN_PASSWORD || "admin1234";

  if (USE_MONGO) {
    defineModels();
    const exists = await UserModel.findOne({ role: "admin" }).lean();
    if (exists) return;
    const passwordHash = bcrypt.hashSync(password, 10);
    await UserModel.create({
      id: cryptoRandomId(),
      role: "admin",
      username,
      email,
      passwordHash,
      createdAt: nowIso(),
    });
    console.log(`[seed] admin created: ${username}`);
    return;
  }

  const users = readJson(USERS_JSON, []);
  if (users.some((u) => u.role === "admin")) return;
  const passwordHash = bcrypt.hashSync(password, 10);
  users.push({
    id: cryptoRandomId(),
    role: "admin",
    username,
    email,
    passwordHash,
    createdAt: nowIso(),
  });
  writeJsonAtomic(USERS_JSON, users);
  console.log(`[seed] admin created: ${username}`);
}

async function ensureAdminUsername() {
  const username = process.env.ADMIN_ID || "admin";
  if (USE_MONGO) {
    defineModels();
    const admin = await UserModel.findOne({ role: "admin" });
    if (!admin) return;
    if (admin.username && String(admin.username).trim()) return;
    admin.username = username;
    await admin.save();
    console.log(`[migrate] admin username set: ${username}`);
    return;
  }

  const users = readJson(USERS_JSON, []);
  const admin = users.find((u) => u.role === "admin");
  if (!admin) return;
  if (admin.username && String(admin.username).trim()) return;
  admin.username = username;
  writeJsonAtomic(USERS_JSON, users);
  console.log(`[migrate] admin username set: ${username}`);
}

function cryptoRandomId() {
  // avoid requiring node:crypto for old node; simple unique-enough id for local use
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

function requireAdmin(req, res, next) {
  if (req.session?.user?.role === "admin") return next();
  return res.redirect("/admin/login");
}

app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "views"));

app.use(express.urlencoded({ extended: true }));
app.use(express.json());

app.use(
  session({
    secret: SESSION_SECRET,
    resave: false,
    saveUninitialized: false,
    cookie: {
      httpOnly: true,
      sameSite: "lax",
      secure: false,
      maxAge: 24 * 60 * 60 * 1000,
    },
  })
);

app.use((req, res, next) => {
  res.locals.user = req.session?.user || null;
  next();
});

app.use("/uploads", express.static(UPLOADS_DIR));
app.use("/image", express.static(IMAGE_DIR));
app.use("/static", express.static(path.join(__dirname, "static")));

function createDiskUpload() {
  return multer({
    storage: multer.diskStorage({
      destination: (req, file, cb) => cb(null, UPLOADS_DIR),
      filename: (req, file, cb) => {
        const safeBase = path
          .basename(file.originalname)
          .replace(/[^\w.\- ]+/g, "_")
          .slice(-120);
        cb(null, `${Date.now()}-${safeBase}`);
      },
    }),
    limits: { fileSize: 5 * 1024 * 1024 },
  });
}

function createMemoryUpload() {
  return multer({
    storage: multer.memoryStorage(),
    limits: { fileSize: 5 * 1024 * 1024 },
  });
}

const uploadCsv = createDiskUpload(); // csv/xlsx import uses temp file
const uploadImages = USE_CLOUDINARY ? createMemoryUpload() : createDiskUpload();

if (USE_CLOUDINARY) {
  cloudinary.config(
    CLOUDINARY_URL
      ? { secure: true }
      : {
          cloud_name: CLOUDINARY_CLOUD_NAME,
          api_key: CLOUDINARY_API_KEY,
          api_secret: CLOUDINARY_API_SECRET,
          secure: true,
        }
  );
}

async function uploadImageToCloudinary(file, opts = {}) {
  if (!USE_CLOUDINARY) throw new Error("Cloudinary is not configured");
  if (!file?.buffer) throw new Error("No file buffer");
  const folder = opts.folder || CLOUDINARY_FOLDER;
  const publicIdPrefix = opts.publicIdPrefix || "";
  let public_id = "";

  if (CLOUDINARY_DEDUP) {
    const hash = crypto.createHash("sha256").update(file.buffer).digest("hex");
    public_id = `sha256-${hash}`;
  } else {
    const base = path.basename(file.originalname || "image").replace(/\.[^/.]+$/, "");
    const safeBase = base.replace(/[^\w.\- ]+/g, "_").slice(-80);
    public_id = `${publicIdPrefix}${Date.now()}-${safeBase}`.replace(/\s+/g, "_");
  }

  try {
    const result = await new Promise((resolve, reject) => {
      const stream = cloudinary.uploader.upload_stream(
        {
          folder,
          public_id,
          resource_type: "image",
          overwrite: false,
        },
        (err, r) => {
          if (err) return reject(err);
          resolve(r);
        }
      );
      stream.end(file.buffer);
    });
    return { url: result.secure_url, publicId: result.public_id };
  } catch (e) {
    // If dedup is enabled and the asset already exists, reuse it.
    if (CLOUDINARY_DEDUP) {
      const fullPublicId = `${folder}/${public_id}`;
      try {
        const existing = await cloudinary.api.resource(fullPublicId, { resource_type: "image" });
        return { url: existing.secure_url, publicId: existing.public_id };
      } catch {
        // fallthrough
      }
    }
    throw e;
  }
}

async function deleteCloudinaryImage(publicId) {
  if (!USE_CLOUDINARY) return;
  // When using content-hash dedup, the same asset may be referenced by multiple prizes.
  // For safety, we only remove the reference (DB field) and keep the asset.
  if (CLOUDINARY_DEDUP) return;
  const pid = String(publicId || "").trim();
  if (!pid) return;
  try {
    await cloudinary.uploader.destroy(pid, { resource_type: "image" });
  } catch (e) {
    console.warn("[cloudinary] destroy warning:", e?.message || e);
  }
}

function listPrizes() {
  if (USE_MONGO) {
    // This function becomes async via listPrizesAsync; keep for backward safety.
    return [];
  }
  const prizes = readJson(PRIZES_JSON, []);
  return Array.isArray(prizes) ? prizes : [];
}

function savePrizes(prizes) {
  if (USE_MONGO) return;
  writeJsonAtomic(PRIZES_JSON, prizes);
}

async function listPrizesAsync() {
  if (USE_MONGO) {
    defineModels();
    const docs = await PrizeModel.find({}).lean();
    return docs || [];
  }
  return listPrizes();
}

async function findPrizeById(id) {
  if (USE_MONGO) {
    defineModels();
    return await PrizeModel.findOne({ id }).lean();
  }
  const prizes = listPrizes();
  return prizes.find((p) => p.id === id) || null;
}

async function insertPrize(prize) {
  if (USE_MONGO) {
    defineModels();
    await PrizeModel.create(prize);
    return;
  }
  const prizes = listPrizes();
  prizes.push(prize);
  savePrizes(prizes);
}

async function updatePrizeById(id, patch) {
  if (USE_MONGO) {
    defineModels();
    await PrizeModel.updateOne({ id }, { $set: patch });
    return;
  }
  const prizes = listPrizes();
  const idx = prizes.findIndex((p) => p.id === id);
  if (idx < 0) return;
  prizes[idx] = { ...prizes[idx], ...patch };
  savePrizes(prizes);
}

async function deletePrizeById(id) {
  if (USE_MONGO) {
    defineModels();
    await PrizeModel.deleteOne({ id });
    return;
  }
  const prizes = listPrizes();
  savePrizes(prizes.filter((p) => p.id !== id));
}

function parseNumberLoose(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  if (!s) return null;
  const cleaned = s.replace(/[^\d.\-]/g, "");
  if (!cleaned) return null;
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

function normalizeCellText(v) {
  if (v === null || v === undefined) return "";
  return String(v).replace(/\s+/g, " ").trim();
}

function stripBrackets(s) {
  const t = String(s || "").trim();
  const m = t.match(/^[\[\(（【「『](.*)[\]\)）】」』]$/);
  return (m ? m[1] : t).trim();
}

function guessSetNameFromTitle(title) {
  const t = String(title || "");
  const m = t.match(/セット\s*([A-Za-zＡ-Ｚａ-ｚ])/);
  if (!m) return "";
  const letter = m[1].toUpperCase().replace(/[Ａ-Ｚ]/g, (ch) => String.fromCharCode(ch.charCodeAt(0) - 0xfee0));
  return `${letter}セット`;
}

function isLikelyLegacyBlockSheet(aoa) {
  // Legacy: no header row with "title", but has bracketed category like [テーマパーク] and rows like TDRO3A-2405
  const flat = aoa.flat().slice(0, 200).map(normalizeCellText).filter(Boolean);
  const hasTitleHeader = flat.some((x) => x.toLowerCase() === "title" || x.toLowerCase() === "setname");
  if (hasTitleHeader) return false;
  const hasBracketCategory = flat.some((x) => /^[\[\(（【「『].+[\]\)）】」』]$/.test(x));
  const hasCodeLike = flat.some((x) => /^[A-Za-z]{2,}\d+[A-Za-z]?(?:[-－ー]\d{3,})?$/.test(x));
  return hasBracketCategory && hasCodeLike;
}

function parseLegacyBlockAoA(aoa) {
  // Legacy "block" format. Columns may shift due to merged cells; we scan the whole row.
  // We parse groups:
  // - Category row: A like [テーマパーク]
  // - Group header row: A code like TDRO3A-2405, B like [ディズニー...], C has 販売価格
  // - Item rows: B starts with "・" or bullet-like, C has line price (optional)
  // - Total row: B empty, C numeric total
  let currentCategory = "";
  const prizes = [];

  let current = null; // { title, category, setName, items: [], total: number|null }
  const BRACKET_RE = /^[\[\(（【「『].+[\]\)）】」』]$/;
  // Examples: TDRO3A-2405, TDRO3A－2405, JCB03A (no hyphen)
  const CODE_RE = /^[A-Za-z]{2,}\d+[A-Za-z]?(?:[-－ー]\d{3,})?$/;

  function flush() {
    if (!current) return;
    const title = normalizeCellText(current.title);
    if (!title) {
      current = null;
      return;
    }
    const description = current.items
      .map((x) => normalizeCellText(x).replace(/^[・•\-\u2212]\s*/, ""))
      .filter(Boolean)
      .join(" / ");
    const priceYen = current.total ?? 0;
    const setName = current.setName || guessSetNameFromTitle(title);
    prizes.push({
      id: cryptoRandomId(),
      title,
      description,
      category: current.category || "",
      setName,
      tags: [],
      priceYen,
      quantity: 0,
      imagePath: "",
      createdAt: nowIso(),
      updatedAt: nowIso(),
    });
    current = null;
  }

  function rowCells(row) {
    return (row || []).map(normalizeCellText);
  }

  function nonEmptyCells(cells) {
    return cells.map((v, idx) => ({ v, idx })).filter((x) => x.v);
  }

  function findFirstIndex(cells, pred) {
    for (let i = 0; i < cells.length; i++) if (pred(cells[i], i)) return i;
    return -1;
  }

  function pickFirst(cells, pred) {
    const idx = findFirstIndex(cells, pred);
    return idx >= 0 ? cells[idx] : "";
  }

  function extractRowNumbers(cells) {
    const nums = [];
    for (const v of cells) {
      const n = parseNumberLoose(v);
      if (n !== null) nums.push(n);
    }
    return nums;
  }

  for (let i = 0; i < aoa.length; i++) {
    const cells = rowCells(aoa[i]);
    const nonEmpty = nonEmptyCells(cells);

    // Category row like [テーマパーク]
    if (nonEmpty.length === 1 && BRACKET_RE.test(nonEmpty[0].v)) {
      currentCategory = stripBrackets(nonEmpty[0].v);
      continue;
    }

    // Start of a group: a code-like cell + a title cell (usually bracketed)
    const codeIdx = findFirstIndex(cells, (v) => CODE_RE.test(v));
    const bracketTitleIdx = findFirstIndex(cells, (v) => BRACKET_RE.test(v));
    const titleFallbackIdx =
      bracketTitleIdx >= 0
        ? bracketTitleIdx
        : findFirstIndex(cells, (v, idx) => idx !== codeIdx && /セット/.test(v));

    if (codeIdx >= 0 && titleFallbackIdx >= 0) {
      flush();
      const rawTitle = cells[titleFallbackIdx];
      const title = stripBrackets(rawTitle);
      current = {
        title,
        category: currentCategory,
        setName: guessSetNameFromTitle(title),
        items: [],
        total: null,
      };
      continue;
    }

    if (!current) continue;

    // Item row: first cell that looks like a bullet list entry
    const item = pickFirst(cells, (v) => /^[・•\-\u2212]/.test(v));
    if (item) {
      current.items.push(item);
      continue;
    }

    // Total row: a row that mainly contains a number (often the sum)
    const nums = extractRowNumbers(cells);
    if (nums.length) {
      // Heuristic: if there's a single number and little other text, treat it as total
      const hasOtherText = nonEmpty.some((x) => parseNumberLoose(x.v) === null);
      if (!hasOtherText || nonEmpty.length <= 2) {
        current.total = Math.max(...nums);
        continue;
      }
    }
  }

  flush();
  return prizes;
}

function findUserByIdentifier(identifier) {
  if (USE_MONGO) return null; // use async version
  const users = readJson(USERS_JSON, []);
  const key = String(identifier || "").toLowerCase();
  return users.find((u) => {
    const username = String(u.username || "").toLowerCase();
    const email = String(u.email || "").toLowerCase();
    return username === key || email === key;
  });
}

async function findUserByIdentifierAsync(identifier) {
  const key = String(identifier || "").toLowerCase();
  if (USE_MONGO) {
    defineModels();
    return await UserModel.findOne({
      $or: [{ username: key }, { email: key }],
    }).lean();
  }
  return findUserByIdentifier(identifier);
}

// ---- Public ----
app.get("/", (req, res) => res.redirect("/prizes"));

app.get("/prizes", async (req, res) => {
  const q = String(req.query.q || "").trim().toLowerCase();
  const quantityStr = String(req.query.quantity || "").trim();
  const quantity = quantityStr ? Number(quantityStr) : null;
  const priceRange = String(req.query.priceRange || "").trim();

  const prizes = await listPrizesAsync();
  const filtered = prizes.filter((p) => {
    const matchesQ =
      !q ||
      String(p.title || "").toLowerCase().includes(q);
    const matchesQuantity =
      quantityStr === "" ||
      (() => {
        const title = String(p.title || "");
        // If user typed a number, match either quantity field or number contained in title.
        if (Number.isFinite(quantity)) {
          if (Number(p.quantity || 0) === quantity) return true;
          const n = String(quantity);
          const nFw = n.replace(/[0-9]/g, (d) => String.fromCharCode(d.charCodeAt(0) + 0xfee0)); // ３など
          return title.includes(n) || title.includes(nFw);
        }
        // If user typed non-numeric, treat it as substring against title.
        return title.includes(quantityStr);
      })();
    const matchesPrice =
      !priceRange ||
      (() => {
        const price = Number(p.priceYen || 0);
        if (!Number.isFinite(price) || price <= 0) return false;
        const m = priceRange.match(/^(\d+)-(\d+)?$/);
        if (!m) return true;
        const min = Number(m[1]);
        const max = m[2] ? Number(m[2]) : null;
        if (!Number.isFinite(min)) return true;
        if (max === null) return price >= min;
        return price >= min && price < max;
      })();
    return matchesQ && matchesQuantity && matchesPrice;
  });

  res.render("public_index", { prizes: filtered, q, quantityStr, priceRange });
});

app.get("/prizes/:id", async (req, res) => {
  const prize = await findPrizeById(req.params.id);
  if (!prize) return res.status(404).send("Not found");
  res.render("public_detail", { prize });
});

// ---- Admin auth ----
app.get("/admin", (req, res) => res.redirect("/admin/prizes"));

app.get("/admin/login", (req, res) => {
  res.render("admin_login", { error: null });
});

app.post("/admin/login", async (req, res) => {
  const { username, password } = req.body || {};
  const user = await findUserByIdentifierAsync(username || "");
  if (!user) return res.status(401).render("admin_login", { error: "ログイン情報が違います" });

  const ok = await bcrypt.compare(String(password || ""), user.passwordHash);
  if (!ok) return res.status(401).render("admin_login", { error: "ログイン情報が違います" });

  req.session.user = { id: user.id, role: user.role, username: user.username || "", email: user.email || "" };
  res.redirect("/admin/prizes");
});

app.post("/admin/logout", (req, res) => {
  req.session.destroy(() => res.redirect("/admin/login"));
});

// ---- Admin prizes ----
app.get("/admin/prizes", requireAdmin, async (req, res) => {
  const q = String(req.query.q || "").trim().toLowerCase();
  const prizes = await listPrizesAsync();
  const filtered = !q
    ? prizes
    : prizes.filter(
        (p) =>
          String(p.title || "").toLowerCase().includes(q)
      );
  res.render("admin_prizes", { prizes: filtered, q, user: req.session.user });
});

app.get("/admin/prizes/new", requireAdmin, (req, res) => {
  res.render("admin_prize_form", { mode: "new", prize: null, error: null, user: req.session.user });
});

app.get("/admin/quick-add", requireAdmin, (req, res) => {
  res.render("admin_quick_add", { error: null, form: {}, user: req.session.user });
});

app.post("/admin/quick-add", requireAdmin, uploadImages.single("image"), async (req, res) => {
  try {
    const { title, priceYen } = req.body || {};
    if (!title) {
      return res.status(400).render("admin_quick_add", {
        error: "名前（タイトル）は必須です",
        form: { title, priceYen },
        user: req.session.user,
      });
    }

    let imagePath = DEFAULT_IMAGE_PATH;
    let imagePublicId = "";
    if (req.file) {
      if (USE_CLOUDINARY) {
        const uploaded = await uploadImageToCloudinary(req.file, { publicIdPrefix: "main-" });
        imagePath = uploaded.url;
        imagePublicId = uploaded.publicId;
      } else {
        imagePath = `/uploads/${req.file.filename}`;
      }
    }

    await insertPrize({
      id: cryptoRandomId(),
      title: String(title).trim(),
      description: "",
      category: "",
      setName: "",
      priceYen: priceYen ? Number(priceYen) : 0,
      quantity: 0,
      tags: [],
      imagePath,
      imagePublicId,
      detailImagePath: "",
      detailImagePublicId: "",
      createdAt: nowIso(),
      updatedAt: nowIso(),
    });
    res.redirect("/admin/prizes");
  } catch (e) {
    res.status(500).send(String(e));
  }
});

app.post("/admin/prizes/new", requireAdmin, uploadImages.fields([{ name: "image", maxCount: 1 }, { name: "detailImage", maxCount: 1 }]), async (req, res) => {
  try {
    const { title, description, category, tags, setName, priceYen, quantity } = req.body || {};
    if (!title) {
      return res.status(400).render("admin_prize_form", {
        mode: "new",
        prize: null,
        error: "タイトルは必須です",
        user: req.session.user,
      });
    }

    const mainFile = req.files?.image?.[0] || null;
    const detailFile = req.files?.detailImage?.[0] || null;

    let imagePath = DEFAULT_IMAGE_PATH;
    let imagePublicId = "";
    if (mainFile) {
      if (USE_CLOUDINARY) {
        const uploaded = await uploadImageToCloudinary(mainFile, { publicIdPrefix: "main-" });
        imagePath = uploaded.url;
        imagePublicId = uploaded.publicId;
      } else {
        imagePath = `/uploads/${mainFile.filename}`;
      }
    }

    let detailImagePath = "";
    let detailImagePublicId = "";
    if (detailFile) {
      if (USE_CLOUDINARY) {
        const uploaded = await uploadImageToCloudinary(detailFile, { publicIdPrefix: "detail-" });
        detailImagePath = uploaded.url;
        detailImagePublicId = uploaded.publicId;
      } else {
        detailImagePath = `/uploads/${detailFile.filename}`;
      }
    }

    const prize = {
      id: cryptoRandomId(),
      title: String(title).trim(),
      description: String(description || "").trim(),
      category: String(category || "").trim(),
      setName: String(setName || "").trim(),
      priceYen: priceYen ? Number(priceYen) : 0,
      quantity: quantity ? Number(quantity) : 0,
      tags: String(tags || "")
        .split(",")
        .map((t) => t.trim())
        .filter(Boolean),
      imagePath,
      imagePublicId,
      detailImagePath,
      detailImagePublicId,
      createdAt: nowIso(),
      updatedAt: nowIso(),
    };
    await insertPrize(prize);
    res.redirect("/admin/prizes");
  } catch (e) {
    res.status(500).send(String(e));
  }
});

app.get("/admin/prizes/:id/edit", requireAdmin, async (req, res) => {
  const prize = await findPrizeById(req.params.id);
  if (!prize) return res.status(404).send("Not found");
  res.render("admin_prize_form", { mode: "edit", prize, error: null, user: req.session.user });
});

app.post("/admin/prizes/:id/edit", requireAdmin, uploadImages.fields([{ name: "image", maxCount: 1 }, { name: "detailImage", maxCount: 1 }]), async (req, res) => {
  const current = await findPrizeById(req.params.id);
  if (!current) return res.status(404).send("Not found");

  const { title, description, category, tags, setName, priceYen, quantity } = req.body || {};
  const mainFile = req.files?.image?.[0] || null;
  const detailFile = req.files?.detailImage?.[0] || null;
  const removeDetailImage = String(req.body?.removeDetailImage || "") === "1";

  if (!title) {
    return res.status(400).render("admin_prize_form", {
      mode: "edit",
      prize: { ...current, ...req.body },
      error: "タイトルは必須です",
      user: req.session.user,
    });
  }

  let nextImagePath = current.imagePath;
  let nextImagePublicId = current.imagePublicId || "";
  if (mainFile) {
    if (USE_CLOUDINARY) {
      const uploaded = await uploadImageToCloudinary(mainFile, { publicIdPrefix: "main-" });
      nextImagePath = uploaded.url;
      // delete old only if it was cloudinary-managed
      await deleteCloudinaryImage(nextImagePublicId);
      nextImagePublicId = uploaded.publicId;
    } else {
      nextImagePath = `/uploads/${mainFile.filename}`;
      nextImagePublicId = "";
    }
  }

  let nextDetailImagePath = current.detailImagePath || "";
  let nextDetailImagePublicId = current.detailImagePublicId || "";
  if (detailFile) {
    if (USE_CLOUDINARY) {
      const uploaded = await uploadImageToCloudinary(detailFile, { publicIdPrefix: "detail-" });
      await deleteCloudinaryImage(nextDetailImagePublicId);
      nextDetailImagePath = uploaded.url;
      nextDetailImagePublicId = uploaded.publicId;
    } else {
      nextDetailImagePath = `/uploads/${detailFile.filename}`;
      nextDetailImagePublicId = "";
    }
  } else if (removeDetailImage) {
    await deleteCloudinaryImage(nextDetailImagePublicId);
    nextDetailImagePath = "";
    nextDetailImagePublicId = "";
  }

  await updatePrizeById(req.params.id, {
    title: String(title).trim(),
    description: String(description || "").trim(),
    category: String(category || "").trim(),
    setName: String(setName || "").trim(),
    priceYen: priceYen ? Number(priceYen) : 0,
    quantity: quantity ? Number(quantity) : 0,
    tags: String(tags || "")
      .split(",")
      .map((t) => t.trim())
      .filter(Boolean),
    imagePath: nextImagePath,
    imagePublicId: nextImagePublicId,
    detailImagePath: nextDetailImagePath,
    detailImagePublicId: nextDetailImagePublicId,
    updatedAt: nowIso(),
  });
  res.redirect("/admin/prizes");
});

app.post("/admin/prizes/:id/delete", requireAdmin, async (req, res) => {
  await deletePrizeById(req.params.id);
  res.redirect("/admin/prizes");
});

// ---- CSV import (admin) ----
app.get("/admin/import", requireAdmin, (req, res) => {
  res.render("admin_import", { error: null, user: req.session.user });
});

app.post("/admin/import", requireAdmin, uploadCsv.single("csv"), async (req, res) => {
  if (!req.file) return res.status(400).render("admin_import", { error: "CSVを選択してください", user: req.session.user });

  const csvPath = path.join(UPLOADS_DIR, req.file.filename);
  const rows = [];

  try {
    const ext = path.extname(req.file.originalname || "").toLowerCase();
    const isExcel = ext === ".xlsx" || ext === ".xls" || ext === ".xlsm";

    if (isExcel) {
      const wb = XLSX.readFile(csvPath, { cellDates: true });
      const firstSheetName = wb.SheetNames?.[0];
      if (!firstSheetName) {
        return res.status(400).render("admin_import", { error: "Excelにシートがありません", user: req.session.user });
      }
      const ws = wb.Sheets[firstSheetName];
      const jsonRows = XLSX.utils.sheet_to_json(ws, { defval: "", raw: false });
      const hasHeaderStyle = jsonRows.length > 0 && Object.keys(jsonRows[0] || {}).some((k) => String(k).toLowerCase() === "title");
      if (hasHeaderStyle) {
        rows.push(...jsonRows);
      } else {
        const aoa = XLSX.utils.sheet_to_json(ws, { header: 1, defval: "", raw: false });
        const legacy = isLikelyLegacyBlockSheet(aoa) ? parseLegacyBlockAoA(aoa) : [];
        if (!legacy.length) {
          return res.status(400).render("admin_import", {
            error: "Excel形式を判定できませんでした。1行目ヘッダーの表形式（title列あり）にするか、CSVで取り込んでください。",
            user: req.session.user,
          });
        }
        // Convert to row objects so the same mapping logic can be used below
        legacy.forEach((p) =>
          rows.push({
            title: p.title,
            description: p.description,
            category: p.category,
            setName: p.setName,
            tags: (p.tags || []).join(","),
            priceYen: p.priceYen,
            quantity: p.quantity,
          })
        );
      }
    } else {
      await new Promise((resolve, reject) => {
        fs.createReadStream(csvPath)
          .pipe(
            parse({
              columns: true,
              skip_empty_lines: true,
              trim: true,
            })
          )
          .on("data", (r) => rows.push(r))
          .on("end", resolve)
          .on("error", reject);
      });
    }

    const toAdd = rows.map((r) => ({
      id: cryptoRandomId(),
      title: String(r.title || r.name || "").trim(),
      description: String(r.description || "").trim(),
      category: String(r.category || "").trim(),
      setName: String(r.setName || r.set || "").trim(),
      tags: String(r.tags || "")
        .split(/[,、]/)
        .map((t) => t.trim())
        .filter(Boolean),
      priceYen: parseNumberLoose(r.priceYen ?? r.price ?? r.price_yen) ?? 0,
      quantity: parseNumberLoose(r.quantity ?? r.qty) ?? 0,
      imagePath: DEFAULT_IMAGE_PATH,
      imagePublicId: "",
      detailImagePath: "",
      detailImagePublicId: "",
      createdAt: nowIso(),
      updatedAt: nowIso(),
    })).filter((p) => p.title);

    if (USE_MONGO) {
      defineModels();
      try {
        await PrizeModel.insertMany(toAdd, { ordered: false });
      } catch (e) {
        // Ignore duplicate key errors etc. (best-effort import)
        console.warn("[import] insertMany warning:", e?.message || e);
      }
    } else {
      const prizes = listPrizes();
      savePrizes([...prizes, ...toAdd]);
    }
    res.redirect("/admin/prizes");
  } catch (e) {
    res.status(500).render("admin_import", { error: `CSV取り込みに失敗しました: ${e}`, user: req.session.user });
  } finally {
    // keep uploaded CSV? remove to avoid clutter
    try { fs.unlinkSync(csvPath); } catch {}
  }
});

async function bootstrap() {
  ensureDirs();
  if (USE_MONGO) {
    try {
      await initDbIfNeeded();
    } catch (e) {
      console.error("❌ MongoDB接続に失敗しました:", e);
      process.exit(1);
    }
  }
  await seedAdminIfNeeded();
  await ensureAdminUsername();

  app.listen(PORT, () => {
    console.log(`prizes app listening on http://localhost:${PORT}`);
    console.log(`public: http://localhost:${PORT}/prizes`);
    console.log(`admin:  http://localhost:${PORT}/admin/login`);
    if (USE_MONGO) console.log("storage: MongoDB");
    else console.log("storage: JSON files (local)");
  });
}

bootstrap();

