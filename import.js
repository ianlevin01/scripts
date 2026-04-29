import xlsx from "xlsx";
import pkg from "pg";

const { Client } = pkg;

const client = new Client({
  connectionString: "postgresql://postgres:OnpiysoN00bnd9%3D%3F@database-1.c3eyesi4ucpy.sa-east-1.rds.amazonaws.com:5432/postgres",
  ssl: { rejectUnauthorized: false }
});

await client.connect();

const parseNumber = (val) => {
  if (val === null || val === undefined || val === "") return null;
  const num = parseFloat(String(val).replace(",", "."));
  return isNaN(num) ? null : num;
};

const NEGOCIO_ID = "00000000-0000-0000-0000-000000000001";

const warehouseNames = [
  "Pasteur 102","Pasteur 280","Peron Lejos","Oficina",
  "Oficina ML","Camarin","Tertulia","Salon Teatro",
  "Escenario","Despacho"
];

const warehouses = {};

for (const name of warehouseNames) {
  const res = await client.query(`
    INSERT INTO warehouses (name, negocio_id)
    VALUES ($1, $2)
    ON CONFLICT (negocio_id, name) DO NOTHING
    RETURNING id
  `, [name, NEGOCIO_ID]);

  if (res.rows[0]) {
    warehouses[name] = res.rows[0].id;
  } else {
    const existing = await client.query(
      `SELECT id FROM warehouses WHERE name = $1 AND negocio_id = $2`,
      [name, NEGOCIO_ID]
    );
    warehouses[name] = existing.rows[0]?.id;
  }
}

const categoryMap = {};
const categoryRes = await client.query(
  `SELECT id, name FROM categories WHERE negocio_id = $1`, [NEGOCIO_ID]
);
for (const cat of categoryRes.rows) {
  categoryMap[cat.name.trim().toLowerCase()] = cat.id;
}

const workbook = xlsx.readFile("productos.xlsx");
const sheet = workbook.Sheets[workbook.SheetNames[0]];
const rows = xlsx.utils.sheet_to_json(sheet, { header: 1 });

console.log("Filas totales:", rows.length);

const headerIndex = rows.findIndex(r =>
  r.includes("CODIGO") && r.includes("DETALLE")
);

if (headerIndex === -1) throw new Error("No se encontró header real");

const headers = rows[headerIndex];
console.log("Header encontrado en fila:", headerIndex);
console.log("Columnas detectadas:", headers); // 👈 útil para verificar nombres de precios

const idx = {
  code:      headers.indexOf("CODIGO"),
  detalle:   headers.indexOf("DETALLE"),
  qxb:       headers.indexOf("QxB"),
  costo:     headers.indexOf("Costo"),
  rubro:     headers.indexOf("Rubro"),
  ptoPedido: headers.indexOf("Pto Pedido"),
};

// Stock columns start 6 positions after costo (1 costo + 5 precio columns skipped)
const precioStartIdx = idx.costo + 1;
const CANTIDAD_PRECIOS = 5;

console.log("Indices detectados:", idx);

const data = rows.slice(headerIndex + 1);

let inserted = 0;
let updated = 0;

for (const row of data) {
  const code = row[idx.code];
  if (!code) continue;

  const ptoPedido = parseNumber(row[idx.ptoPedido]);
  if (!ptoPedido || ptoPedido <= 0) {
    console.log(`⏭️ ${code} omitido (Pto Pedido = ${ptoPedido})`);
    continue;
  }

  const detalle   = row[idx.detalle];
  const qxb       = parseNumber(row[idx.qxb]);
  const costo     = parseNumber(row[idx.costo]);
  const rubroRaw  = row[idx.rubro];
  const rubro     = rubroRaw?.toString().trim().toLowerCase();
  const categoryId = categoryMap[rubro] || null;

  console.log(`→ ${code} | rubro: ${rubro}`);

  // producto
  let res = await client.query(
    `SELECT id FROM products WHERE code = $1 AND negocio_id = $2`, [code, NEGOCIO_ID]
  );

  let productId;

  if (res.rows.length) {
    productId = res.rows[0].id;
    await client.query(`
      UPDATE products
      SET name = $1, qxb = $2, category_id = $3
      WHERE id = $4
    `, [detalle, qxb, categoryId, productId]);
    updated++;
  } else {
    const insert = await client.query(`
      INSERT INTO products (code, name, qxb, category_id, negocio_id)
      VALUES ($1, $2, $3, $4, $5)
      RETURNING id
    `, [code, detalle, qxb, categoryId, NEGOCIO_ID]);
    productId = insert.rows[0].id;
    inserted++;
  }

  // costo
  if (costo !== null) {
    await client.query(`
      INSERT INTO product_costs (product_id, cost)
      VALUES ($1, $2)
    `, [productId, costo]);
  }

  // stock (10 warehouses)
  const stockStartIdx = precioStartIdx + CANTIDAD_PRECIOS;
  for (let i = 0; i < warehouseNames.length; i++) {
    const quantity = parseNumber(row[stockStartIdx + i]);
    if (quantity !== null) {
      await client.query(`
        INSERT INTO stock (product_id, warehouse_id, quantity)
        VALUES ($1, $2, $3)
        ON CONFLICT (product_id, warehouse_id)
        DO UPDATE SET quantity = EXCLUDED.quantity
      `, [productId, warehouses[warehouseNames[i]], quantity]);
    }
  }
}

await client.end();

console.log("Insertados:", inserted);
console.log("Actualizados:", updated);