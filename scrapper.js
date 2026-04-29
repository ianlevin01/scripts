import dotenv from "dotenv";
import { fileURLToPath } from "url";
import path from "path";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.join(__dirname, "../sistema_once_backend/.env") });

import puppeteer from "puppeteer";
import pkg from "pg";
import axios from "axios";
import S3Service from "../sistema_once_backend/src/services/S3Service.js";

const { Client } = pkg;

const BASE_URL = "https://oncepuntos.com.ar/";

const client = new Client({
  connectionString: "postgresql://postgres:OnpiysoN00bnd9%3D%3F@database-1.c3eyesi4ucpy.sa-east-1.rds.amazonaws.com:5432/postgres",
  ssl: { rejectUnauthorized: false }
});

const s3 = new S3Service();

// 🔹 descarga imagen → formato compatible con tu S3Service
async function descargarImagen(url) {
  const res = await axios.get(url, {
    responseType: "arraybuffer"
  });

  return {
    buffer: Buffer.from(res.data),
    mimetype: res.headers["content-type"],
    originalname: url.split("/").pop()
  };
}

(async () => {
  console.log("🔌 Conectando DB...");
  await client.connect();

  const browser = await puppeteer.launch({ headless: true });
  const page = await browser.newPage();

  console.log("🌐 Abriendo web...");
  await page.goto(BASE_URL, { waitUntil: "networkidle2" });

  // ⚠️ scroll por si carga dinámico
  for (let i = 0; i < 5; i++) {
    await page.evaluate(() => window.scrollBy(0, window.innerHeight));
    await new Promise(r => setTimeout(r, 800));
  }

  console.log("🕷️ Scrapeando productos...");

  const productos = await page.$$eval(".caja_producto", (items) => {
    return items.map(el => {
      // 🔹 código (CLAVE)
      const codigoText = el.querySelector(".datos1")?.innerText || "";
      const codigoMatch = codigoText.match(/Cód\.:\s*([A-Z0-9-]+)/i);
      const codigo = codigoMatch ? codigoMatch[1].trim() : null;

      // 🔹 imágenes (todas las del carousel)
      const imagenes = Array.from(
        el.querySelectorAll(".carousel-inner .item a")
      )
        .map(a => a.getAttribute("href"))
        .filter(Boolean);

      return { codigo, imagenes };
    });
  });

  console.log(`📦 Productos encontrados: ${productos.length}`);

  let ok = 0;
  let fail = 0;

  for (const p of productos) {
    if (!p.codigo || !p.imagenes.length) continue;

    console.log(`🔄 ${p.codigo}`);

    // 🔹 buscar producto en DB
    const res = await client.query(
      "SELECT id FROM products WHERE code = $1",
      [p.codigo]
    );

    if (!res.rows.length) {
      console.log(`❌ No existe en DB: ${p.codigo}`);
      fail++;
      continue;
    }

    const productId = res.rows[0].id;

    for (const imgPath of p.imagenes) {
      try {
        const url = imgPath.startsWith("http")
          ? imgPath
          : BASE_URL + imgPath;

        // 🔹 descargar
        const file = await descargarImagen(url);

        // 🔹 subir a S3
        const key = await s3.upload(file);

        // 🔹 evitar duplicados (simple)
        const exists = await client.query(
          "SELECT 1 FROM product_images WHERE product_id = $1 AND key = $2",
          [productId, key]
        );

        if (!exists.rows.length) {
          await client.query(
            "INSERT INTO product_images (product_id, key) VALUES ($1,$2)",
            [productId, key]
          );
        }

        console.log(`   📷 subida: ${url}`);
        ok++;

      } catch (err) {
        console.log(`💥 Error img ${p.codigo}`, err.message);
        fail++;
      }
    }
  }

  await browser.close();
  await client.end();

  console.log("\n📊 RESULTADO:");
  console.log("✅ OK:", ok);
  console.log("❌ FAIL:", fail);
})();