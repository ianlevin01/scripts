import xlsx from "xlsx";
import pkg from "pg";
import fs from "fs";
import * as cheerio from "cheerio";

const { Client } = pkg;

const client = new Client({
  connectionString: "postgresql://postgres:OnpiysoN00bnd9%3D%3F@database-1.c3eyesi4ucpy.sa-east-1.rds.amazonaws.com:5432/postgres",
  ssl: { rejectUnauthorized: false }
});

await client.connect();

// =====================
// UTILS
// =====================

const log = (mod, msg) => console.log(`[${mod}] ${msg}`);

const parseNumber = (val) => {
  if (val === null || val === undefined || val === "") return null;
  const num = parseFloat(String(val).replace(",", "."));
  return isNaN(num) ? null : num;
};

const normalize = (str) => {
  if (!str) return null;

  return str
    .toString()
    .toLowerCase()
    .replace(/^\s*\d+\s*/, "")
    .replace(/[^a-z0-9\s]/g, "")
    .replace(/\s+/g, " ")
    .trim();
};

const esBasura = (str) => {
  if (!str) return true;

  const s = str.toLowerCase();

  return (
    s.includes("saldo") ||
    s.includes("total") ||
    s.includes("general") ||
    s.includes("detalle") ||
    s.trim() === ""
  );
};

const parseDate = (val) => {
  if (!val) return new Date();

  try {
    if (typeof val === "number") {
      const base = new Date(1899, 11, 30);
      return new Date(base.getTime() + val * 86400000);
    }

    if (typeof val === "string") {
      const parts = val.split("/");
      if (parts.length === 3) {
        let [d, m, y] = parts;
        if (y.length === 2) y = "20" + y;
        return new Date(`${y}-${m}-${d}`);
      }
    }

    return new Date(val);
  } catch {
    return new Date();
  }
};

// =====================
// CLIENTES (HTML)
// =====================

async function importClientes() {
  log("CLIENTES", "Importando...");

  const html = fs.readFileSync("clientes.xls", "utf8");
  const $ = cheerio.load(html);

  const rows = [];

  $("table tr").each((i, el) => {
    const cols = [];

    $(el).find("td, th").each((j, cell) => {
      let val = $(cell).text().trim();
      if (val === "_" || val === "---------------") val = null;
      cols.push(val);
    });

    if (cols.length >= 16) rows.push(cols);
  });

  const clientesMap = {};
  const clientesDivisaMap = {};

  for (const cols of rows) {
    try {
      const [
        codigo, detalle, domicilio, localidad, provincia,
        postal, cuit, contacto, telefonos,
        descuento, plazo, email, transporte,
        condicion, vendedor, moneda
      ] = cols;

      if (esBasura(detalle)) continue;

      const nombre = normalize(detalle);
      if (!nombre || clientesMap[nombre]) continue;

      let divisa = "ARS";
      if (moneda && moneda.toLowerCase().includes("dol")) divisa = "USD";

      const telefonoLimpio = telefonos
        ? telefonos.replace(/[^\d]/g, "").slice(0, 15)
        : null;

      const cuitLimpio = cuit
        ? String(cuit).replace(/\D/g, "").slice(0, 20)
        : null;

      const res = await client.query(`
        INSERT INTO customers (
          name, domicilio, localidad, provincia, codigo_postal,
          phone, email, contacto, transporte, condicion_iva,
          vendedor, descuento, dias_plazo, codigo, document, divisa
        )
        VALUES (
          $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16
        )
        RETURNING id
      `, [
        detalle, domicilio, localidad, provincia,
        postal ? String(postal) : null,
        telefonoLimpio, email, contacto, transporte,
        condicion, vendedor,
        parseNumber(descuento),
        parseNumber(plazo),
        codigo ? String(codigo) : null,
        cuitLimpio,
        divisa
      ]);

      // No se crea CC aquí — solo los clientes en corriente_clientes.xls la tendrán
      clientesMap[nombre] = res.rows[0].id;
      clientesDivisaMap[nombre] = divisa;

      log("CLIENTES", `OK ${detalle}`);

    } catch (err) {
      log("CLIENTES", `ERROR: ${err.message}`);
    }
  }

  return { clientesMap, clientesDivisaMap };
}

// =====================
// CTA CLIENTES (🔥 FIX SALDOS)
// =====================

async function importCtaCteClientes(clientesMap, clientesDivisaMap) {
  log("CTA_CLIENTES", "Importando...");

  const wb = xlsx.readFile("corriente_clientes.xls");
  const sheet = wb.Sheets[wb.SheetNames[0]];
  const data = xlsx.utils.sheet_to_json(sheet);

  for (const row of data) {
    try {
      const nombreRaw = row["__EMPTY_1"];

      if (esBasura(nombreRaw)) continue;

      const nombre = normalize(nombreRaw);
      if (!nombre) continue;

      const customerId = clientesMap[nombre];
      const divisa = clientesDivisaMap[nombre] || "ARS";

      if (!customerId) continue;

      const montoARS = parseNumber(row["Saldo $"] ?? row["__EMPTY_2"]);
      const montoUSD = parseNumber(row["Saldo u$s"] ?? row["__EMPTY_3"]);

      let monto = divisa === "USD" ? montoUSD : montoARS;

      if (!monto) continue;

      // Crear CC si no existe aún
      const existingCC = await client.query(
        `SELECT id FROM cuentas_corrientes WHERE customer_id = $1 LIMIT 1`,
        [customerId]
      );
      let cuentaId;
      if (existingCC.rows[0]) {
        cuentaId = existingCC.rows[0].id;
      } else {
        const newCC = await client.query(
          `INSERT INTO cuentas_corrientes (customer_id, saldo, divisa) VALUES ($1, 0, $2) RETURNING id`,
          [customerId, divisa]
        );
        cuentaId = newCC.rows[0].id;
      }

      const fecha = parseDate(row["__EMPTY_4"] || row["__EMPTY_5"]);

      await client.query(`
        INSERT INTO cc_movimientos
        (cuenta_corriente_id, tipo, concepto, monto, created_at)
        VALUES ($1, 'debito', $2, $3, $4)
      `, [cuentaId, divisa, monto, fecha]);

      await client.query(`
        UPDATE cuentas_corrientes
        SET saldo = saldo + $1
        WHERE id = $2
      `, [monto, cuentaId]);

      log("CTA_CLIENTES", `OK ${nombreRaw} → ${divisa} ${monto}`);

    } catch (err) {
      log("CTA_CLIENTES", `ERROR: ${err.message}`);
    }
  }
}

// =====================
// PROVEEDORES
// =====================

async function importProveedores() {
  log("PROVEEDORES", "Importando...");

  const html = fs.readFileSync("proveedores.xls", "utf8");
  const $ = cheerio.load(html);

  const rows = [];
  $("table tr").each((i, el) => {
    const cols = [];
    $(el).find("td, th").each((j, cell) => {
      let val = $(cell).text().trim();
      if (val === "_" || val === "---------------") val = null;
      cols.push(val);
    });
    if (cols.length >= 15) rows.push(cols);
  });

  const map = {};
  const provDivisaMap = {};

  for (const cols of rows) {
    try {
      const [codigo, detalle, , , , , , , , , , , , , moneda] = cols;

      if (esBasura(detalle)) continue;

      const nombre = normalize(detalle);
      if (!nombre || map[nombre]) continue;

      const divisa = moneda && moneda.toLowerCase().includes("dol") ? "USD" : "ARS";

      // Evitar duplicados en re-ejecución
      const existing = await client.query(
        `SELECT id FROM proveedores WHERE name = $1 LIMIT 1`,
        [detalle]
      );

      let provId;
      if (existing.rows[0]) {
        provId = existing.rows[0].id;
        log("PROVEEDORES", `YA EXISTE ${detalle}`);
      } else {
        const res = await client.query(
          `INSERT INTO proveedores (name, divisa) VALUES ($1, $2) RETURNING id`,
          [detalle, divisa]
        );
        provId = res.rows[0].id;
        log("PROVEEDORES", `OK ${detalle} (${divisa})`);
      }

      // No se crea CC aquí — solo los proveedores en corriente_proveedores.xls la tendrán
      map[nombre] = provId;
      provDivisaMap[nombre] = divisa;

    } catch (err) {
      log("PROVEEDORES", `ERROR: ${err.message}`);
    }
  }

  return { map, provDivisaMap };
}

// =====================
// CTA PROVEEDORES
// =====================

async function importCtaCteProveedores(map, provDivisaMap) {
  log("CTA_PROV", "Importando...");

  const wb = xlsx.readFile("corriente_proveedores.xls");
  const sheet = wb.Sheets[wb.SheetNames[0]];
  const data = xlsx.utils.sheet_to_json(sheet);

  for (const row of data) {
    try {
      const nombreRaw = row["Nombre"] || row["__EMPTY_1"];
      if (esBasura(nombreRaw)) continue;

      const nombre = normalize(nombreRaw);
      if (!nombre) continue;

      const provId = map[nombre];
      if (!provId) continue;

      const divisa = provDivisaMap[nombre] || "ARS";

      const montoARS = parseNumber(row["Saldo $"] ?? row["__EMPTY_2"]);
      const montoUSD = parseNumber(row["Saldo u$s"] ?? row["__EMPTY_3"]);
      const monto = divisa === "USD" ? montoUSD : montoARS;

      if (!monto) continue;

      // Crear CC si no existe aún
      const existingCC = await client.query(
        `SELECT id FROM cuentas_corrientes_prov WHERE proveedor_id = $1 LIMIT 1`,
        [provId]
      );
      let cuentaId;
      if (existingCC.rows[0]) {
        cuentaId = existingCC.rows[0].id;
      } else {
        const newCC = await client.query(
          `INSERT INTO cuentas_corrientes_prov (proveedor_id, saldo, divisa) VALUES ($1, 0, $2) RETURNING id`,
          [provId, divisa]
        );
        cuentaId = newCC.rows[0].id;
      }

      await client.query(`
        INSERT INTO cc_movimientos_prov
        (cuenta_corriente_id, tipo, concepto, monto)
        VALUES ($1, 'debito', $2, $3)
      `, [cuentaId, divisa, monto]);

      await client.query(`
        UPDATE cuentas_corrientes_prov
        SET saldo = saldo + $1
        WHERE id = $2
      `, [monto, cuentaId]);

      log("CTA_PROV", `OK ${nombreRaw} → ${divisa} ${monto}`);

    } catch (err) {
      log("CTA_PROV", `ERROR: ${err.message}`);
    }
  }
}

// =====================
// MAIN
// =====================

async function main() {
  try {
    log("MAIN", "Inicio importación");

    const { clientesMap, clientesDivisaMap } = await importClientes();
    await importCtaCteClientes(clientesMap, clientesDivisaMap);

    const { map: proveedoresMap, provDivisaMap } = await importProveedores();
    await importCtaCteProveedores(proveedoresMap, provDivisaMap);

    log("MAIN", "Importación completa ✅");

  } catch (err) {
    log("MAIN", `ERROR GRAVE: ${err.message}`);
  } finally {
    await client.end();
  }
}

main();