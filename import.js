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

const warehouseNames = [
  "Pasteur 102","Pasteur 280","Peron Lejos","Oficina",
  "Oficina ML","Camarin","Tertulia","Salon Teatro",
  "Escenario","Despacho"
];

const warehouses = {};

for (const name of warehouseNames) {
  const res = await client.query(`
    INSERT INTO warehouses (name)
    VALUES ($1)
    ON CONFLICT DO NOTHING
    RETURNING id
  `, [name]);

  if (res.rows[0]) {
    warehouses[name] = res.rows[0].id;
  } else {
    const existing = await client.query(
      `SELECT id FROM warehouses WHERE name = $1`,
      [name]
    );
    warehouses[name] = existing.rows[0]?.id;
  }
}

const categoryMap = {};
const categoryRes = await client.query(`SELECT id, name FROM categories`);
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

// 🔹 Precios: 5 columnas consecutivas después de Costo
// Si en tu Excel tienen nombre exacto, reemplazá por: headers.indexOf("Precio 1"), etc.
const precioStartIdx = idx.costo + 1;
const CANTIDAD_PRECIOS = 5;

console.log("Indices detectados:", idx);
console.log("Precios desde columna índice:", precioStartIdx);

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
    `SELECT id FROM products WHERE code = $1`, [code]
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
      INSERT INTO products (code, name, qxb, category_id)
      VALUES ($1, $2, $3, $4)
      RETURNING id
    `, [code, detalle, qxb, categoryId]);
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

  // 🔹 precios
  for (let i = 0; i < CANTIDAD_PRECIOS; i++) {
    const price = parseNumber(row[precioStartIdx + i]);
    if (price !== null) {
      await client.query(`
        INSERT INTO product_prices (product_id, price_type, price)
        VALUES ($1, $2, $3)
        ON CONFLICT (product_id, price_type)
        DO UPDATE SET price = EXCLUDED.price
      `, [productId, `precio_${i + 1}`, price]);
    }
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