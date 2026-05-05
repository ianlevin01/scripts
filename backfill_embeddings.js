import pkg from "pg";
import OpenAI from "openai";
import dotenv from "dotenv";
import { fileURLToPath } from "url";
import { dirname, resolve } from "path";

dotenv.config({ path: resolve(dirname(fileURLToPath(import.meta.url)), "../sistema_once_backend/.env") });

const { Client } = pkg;
const client = new Client({
  connectionString: "postgresql://postgres:OnpiysoN00bnd9%3D%3F@database-1.c3eyesi4ucpy.sa-east-1.rds.amazonaws.com:5432/postgres",
  ssl: { rejectUnauthorized: false }
});

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

await client.connect();

const { rows: products } = await client.query(`
  SELECT p.id, p.name, p.description, c.name AS category_name
  FROM products p
  LEFT JOIN categories c ON c.id = p.category_id
  WHERE p.embedding IS NULL AND p.deleted_at IS NULL AND p.active = true
  ORDER BY p.name
`);

console.log(`Productos sin embedding: ${products.length}`);

const CHUNK = 10;
let done = 0;
let errors = 0;

for (let i = 0; i < products.length; i += CHUNK) {
  const chunk = products.slice(i, i + CHUNK);

  await Promise.all(chunk.map(async (p) => {
    const text = [p.name, p.description, p.category_name].filter(Boolean).join(". ");
    try {
      const res = await openai.embeddings.create({ model: "text-embedding-3-small", input: text });
      const embedding = res.data[0].embedding;
      await client.query(
        "UPDATE products SET embedding = $1 WHERE id = $2",
        [JSON.stringify(embedding), p.id]
      );
      done++;
      console.log(`[${done}/${products.length}] ${p.name}`);
    } catch (err) {
      errors++;
      console.error(`ERROR ${p.name}: ${err.message}`);
    }
  }));
}

await client.end();
console.log(`\nListo. OK: ${done} | Errores: ${errors}`);
