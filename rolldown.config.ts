import { defineConfig } from 'rolldown'
import path from 'path'
import fs from 'fs'
import { fileURLToPath } from "url";

// Clear the dist folder
const __dirname = path.dirname(fileURLToPath(import.meta.url))
const distPath = path.resolve(__dirname, 'dist')
if (fs.existsSync(distPath)) {
  fs.rmSync(distPath, { recursive: true, force: true })
}

export default defineConfig({
  input: "src/index.ts",
  external: ['ioredis', 'hookable'],
  output: [
    {
      file: "dist/index.mjs",
      format: "esm",
    },
    {
      file: "dist/index.cjs",
      exports: 'named',
      format: "cjs",
    },
  ]
});
