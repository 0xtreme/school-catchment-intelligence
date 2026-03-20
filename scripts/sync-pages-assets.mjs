import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, '..');

const filePairs = [
  ['public/index.html', 'docs/index.html'],
  ['public/app.js', 'docs/app.js'],
  ['public/styles.css', 'docs/styles.css'],
  ['public/favicon.svg', 'docs/favicon.svg'],
];

async function copyFilePair(srcRelative, dstRelative) {
  const src = path.join(projectRoot, srcRelative);
  const dst = path.join(projectRoot, dstRelative);

  await fs.mkdir(path.dirname(dst), { recursive: true });
  await fs.copyFile(src, dst);
  console.log(`Copied ${srcRelative} -> ${dstRelative}`);
}

async function copyDirectory(srcRelative, dstRelative) {
  const src = path.join(projectRoot, srcRelative);
  const dst = path.join(projectRoot, dstRelative);

  await fs.mkdir(path.dirname(dst), { recursive: true });
  await fs.rm(dst, { recursive: true, force: true });
  await fs.cp(src, dst, { recursive: true });
  console.log(`Copied ${srcRelative} -> ${dstRelative}`);
}

async function main() {
  for (const [src, dst] of filePairs) {
    // eslint-disable-next-line no-await-in-loop
    await copyFilePair(src, dst);
  }

  await copyDirectory('public/data', 'docs/data');
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exitCode = 1;
});
