const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');

async function renderSlide(htmlPath, outputPath) {
  const browser = await chromium.launch();
  const page = await browser.newPage();
  await page.setViewportSize({ width: 1080, height: 1350 });

  const html = fs.readFileSync(htmlPath, 'utf8');
  await page.setContent(html, { waitUntil: 'networkidle' });
  await page.waitForTimeout(500); // let fonts load

  await page.screenshot({ path: outputPath, type: 'png' });
  await browser.close();
  console.log(`Rendered: ${outputPath}`);
}

// Render all slides passed as args
const args = process.argv.slice(2);
if (args.length === 0) {
  console.log('Usage: node render.js slide1.html output1.png [slide2.html output2.png ...]');
  process.exit(1);
}

(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage();
  await page.setViewportSize({ width: 1080, height: 1350 });

  for (let i = 0; i < args.length; i += 2) {
    const htmlPath = path.resolve(args[i]);
    const outputPath = args[i + 1];
    const htmlDir = path.dirname(htmlPath);
    await page.goto('file://' + htmlPath, { waitUntil: 'networkidle' });
    await page.waitForTimeout(500);
    await page.screenshot({ path: outputPath, type: 'png' });
    console.log(`Rendered: ${outputPath}`);
  }

  await browser.close();
})();
