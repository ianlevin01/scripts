import { chromium } from "playwright";
import pg from "pg";
const { Client } = pg;

const URL = "https://universomayorista.com/transportes.php";
const PASSWORD_SITIO = "MD2010";

const client = new Client({
  connectionString: "postgresql://postgres:OnpiysoN00bnd9%3D%3F@database-1.c3eyesi4ucpy.sa-east-1.rds.amazonaws.com:5432/postgres",
  ssl: { rejectUnauthorized: false }
});

async function insertarTransporte(d) {
  if (!d.razon_social) return;

  const existe = await client.query(
    `SELECT id FROM transportes WHERE razon_social=$1 AND domicilio=$2`,
    [d.razon_social, d.domicilio]
  );
  if (existe.rows.length) return;

  await client.query(
    `INSERT INTO transportes (codigo, razon_social, domicilio, telefono, email)
     VALUES ($1,$2,$3,$4,$5)`,
    [
      d.codigo,
      d.razon_social,
      d.domicilio,
      d.telefonos,
      d.email
    ]
  );
}

async function extraerDesdeJS(page, value) {
  return await page.evaluate((val) => {
    const data = window.vector_clientes?.[val];
    if (!data) return null;

    return {
      codigo: data[0] || "",
      razon_social: data[1] || "",
      domicilio: data[2] || "",
      localidad: data[3] || "",
      ciudad: data[4] || "",
      telefonos: data[5] || "",
      cuit: data[6] || "",
      email: data[7] || ""
    };
  }, value);
}

async function main() {
  await client.connect();

  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();

  await page.goto(URL);

  // login
  try {
    if (await page.locator('input[type="password"]').isVisible({ timeout: 3000 })) {
      await page.fill('input[type="password"]', PASSWORD_SITIO);
      await page.press('input[type="password"]', "Enter");
      await page.waitForLoadState("domcontentloaded");
    }
  } catch {}

  await page.goto(URL);
  await page.waitForSelector("#lstclientes");

  const opciones = await page.$$eval("#lstclientes option", (opts) =>
    opts
      .map(o => ({ value: o.value, text: o.textContent.trim() }))
      .filter(o => o.text && o.text.length > 1 && !o.text.startsWith("-"))
  );

  for (let i = 0; i < opciones.length; i++) {
    const opcion = opciones[i];
    console.log(`[${i + 1}/${opciones.length}] ${opcion.text}`);

    try {
      const datos = await extraerDesdeJS(page, opcion.value);

      if (!datos || !datos.razon_social) {
        console.log("   ❌ Sin datos");
        continue;
      }

      await insertarTransporte(datos);

    } catch (e) {
      console.log("❌", e.message);
    }
  }

  await browser.close();
  await client.end();
}

main();