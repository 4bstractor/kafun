// Browser-level exercise of the admin UI's batched upload (BatchedUpload
// hook + wave plumbing) — the piece LiveView tests can't drive. Drops N
// generated files on the dropzone of a real page and polls the report
// line until it settles. This caught the v0.4.0 wave races.
//
// Setup (one-time):  npm install playwright && npx playwright install firefox
// Run:               KAFUN_ROOT=/tmp/kafun-repro mix run --no-halt   # bucket "repro" must exist
//                    N=143 node priv/dev/upload_repro.js
const { firefox } = require("playwright");

const N = parseInt(process.env.N || "143", 10);
const URL = "http://localhost:8334/buckets/repro";

(async () => {
  const browser = await firefox.launch();
  const page = await browser.newPage();
  page.on("console", (m) => {
    if (m.type() === "error") console.log("[console.error]", m.text());
  });
  page.on("pageerror", (e) => console.log("[pageerror]", e.message));

  await page.goto(URL);
  await page.waitForSelector("#upload-dropzone");
  // Let the LiveView websocket connect.
  await page.waitForTimeout(1000);

  // Build N tiny files and dispatch a real drop on the dropzone.
  await page.evaluate((n) => {
    const dt = new DataTransfer();
    for (let i = 1; i <= n; i++) {
      const name = `img-${String(i).padStart(3, "0")}.bin`;
      dt.items.add(new File([`payload-${i}-`.repeat(50)], name, { type: "application/octet-stream" }));
    }
    const el = document.getElementById("upload-dropzone");
    el.dispatchEvent(new DragEvent("drop", { bubbles: true, cancelable: true, dataTransfer: dt }));
  }, N);

  // Poll the report line until it stops changing (stall detector).
  let last = "";
  let stableFor = 0;
  for (let i = 0; i < 120; i++) {
    await page.waitForTimeout(1000);
    const report = await page.evaluate(() => {
      const prog = document.querySelector(".upload-report-progress");
      const flash = document.querySelector(".flash");
      const failed = document.querySelector(".upload-failed");
      const rows = document.querySelectorAll(".upload-row").length;
      return [
        prog ? prog.textContent.trim().replace(/\s+/g, " ") : "(no progress line)",
        flash ? "flash: " + flash.textContent.trim().replace(/\s+/g, " ") : "(no flash)",
        failed ? "failed: " + failed.textContent.trim().replace(/\s+/g, " ") : "(no failed list)",
        "pending rows: " + rows,
      ].join(" | ");
    });
    if (report === last) {
      stableFor++;
      if (stableFor >= 8) break; // 8s with no change = settled or stuck
    } else {
      stableFor = 0;
      console.log(`t+${i}s  ${report}`);
      last = report;
    }
  }

  console.log("FINAL:", last);
  await page.screenshot({ path: "repro-final.png", fullPage: false });
  await browser.close();
})();
