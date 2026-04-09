// src/dashboard/ui.js

const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Push Notification Dashboard</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #1a1a1a; color: #e0e0e0; }
    .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
    h1 { margin-bottom: 20px; }
    .section { margin-bottom: 30px; padding: 20px; background: #2a2a2a; border-radius: 8px; }
    h2 { margin-bottom: 15px; color: #4a9eff; }
    .btn { padding: 8px 16px; background: #4a9eff; color: white; border: none; border-radius: 4px; cursor: pointer; margin-right: 10px; }
    .btn:hover { background: #3a8eef; }
    .btn-danger { background: #ff4a4a; }
    .btn-danger:hover { background: #ef3a3a; }
    table { width: 100%; border-collapse: collapse; margin-top: 15px; }
    th, td { padding: 10px; text-align: left; border-bottom: 1px solid #3a3a3a; }
    th { background: #3a3a3a; }
    .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; }
    .stat-card { background: #3a3a3a; padding: 15px; border-radius: 4px; }
    .stat-label { font-size: 12px; color: #888; }
    .stat-value { font-size: 24px; font-weight: bold; }
    .form-group { margin-bottom: 15px; }
    label { display: block; margin-bottom: 5px; }
    input, select, textarea { width: 100%; padding: 8px; background: #3a3a3a; border: 1px solid #4a4a4a; color: #e0e0e0; border-radius: 4px; }
    textarea { min-height: 100px; font-family: monospace; }
  </style>
</head>
<body>
  <div class="container">
    <h1>Push Notification Dashboard</h1>

    <div class="section">
      <h2>Statistics (Last 120 Hours)</h2>
      <div id="stats" class="stats-grid">Loading...</div>
    </div>

    <div class="section">
      <h2>Queue Management</h2>
      <p><a href="/dashboard/queues" class="btn" target="_blank">Open Bull Board</a></p>
      <div id="queue-stats" class="stats-grid">Loading...</div>
    </div>

    <div class="section">
      <h2>Invalid Tokens</h2>
      <div style="margin-bottom: 10px;">
        <input type="text" id="appIdFilter" placeholder="Filter by App ID" style="width: 200px; display: inline-block;">
        <button class="btn" onclick="loadInvalidTokens()">Filter</button>
        <button class="btn btn-danger" onclick="clearInvalidTokens()">Clear All</button>
      </div>
      <div id="token-count"></div>
      <table id="tokens-table">
        <thead>
          <tr><th>App ID</th><th>Token</th><th>Type</th><th>Reason</th><th>Detected</th></tr>
        </thead>
        <tbody id="tokens-body"></tbody>
      </table>
    </div>

    <div class="section">
      <h2>Test Send</h2>
      <form onsubmit="testSend(event)">
        <div class="form-group">
          <label>App ID:</label>
          <select id="test-app-id" required></select>
        </div>
        <div class="form-group">
          <label>Token:</label>
          <input type="text" id="test-token" required>
        </div>
        <div class="form-group">
          <label>Type:</label>
          <select id="test-type" required>
            <option value="ios">iOS</option>
            <option value="android">Android</option>
          </select>
        </div>
        <div class="form-group">
          <label>Priority:</label>
          <select id="test-priority" required>
            <option value="high">High</option>
            <option value="standard">Standard</option>
          </select>
        </div>
        <div class="form-group">
          <label>Request (JSON):</label>
          <textarea id="test-request" required>{
  "payload": {
    "title": "Test Notification",
    "body": "This is a test"
  }
}</textarea>
        </div>
        <button type="submit" class="btn">Send Test Push</button>
      </form>
      <div id="test-result" style="margin-top: 15px;"></div>
    </div>
  </div>

  <script>
    async function loadStats() {
      const res = await fetch('/dashboard/api/stats');
      const data = await res.json();
      const container = document.getElementById('stats');
      container.innerHTML = '';
      for (const [appId, stats] of Object.entries(data.apps)) {
        const totalSuccess = stats.success.reduce((a, b) => a + b, 0);
        const totalFail = stats.fail.reduce((a, b) => a + b, 0);
        container.innerHTML += \`
          <div class="stat-card">
            <div class="stat-label">\${appId}</div>
            <div class="stat-value" style="color: #4eff4a;">\${totalSuccess}</div>
            <div class="stat-label">Success</div>
            <div class="stat-value" style="color: #ff4a4a; font-size: 18px;">\${totalFail}</div>
            <div class="stat-label">Failed</div>
          </div>
        \`;
      }
    }

    async function loadQueueStats() {
      const res = await fetch('/health');
      const data = await res.json();
      const container = document.getElementById('queue-stats');
      container.innerHTML = \`
        <div class="stat-card">
          <div class="stat-label">High Priority</div>
          <div class="stat-value">\${data.queues.high.active}</div>
          <div class="stat-label">Active</div>
        </div>
        <div class="stat-card">
          <div class="stat-label">Standard</div>
          <div class="stat-value">\${data.queues.standard.active}</div>
          <div class="stat-label">Active</div>
        </div>
      \`;
    }

    async function loadInvalidTokens(page = 0) {
      const appId = document.getElementById('appIdFilter').value;
      const url = appId ? \`/dashboard/api/invalid-tokens?app_id=\${appId}&limit=100&offset=\${page * 100}\` : \`/dashboard/api/invalid-tokens?limit=100&offset=\${page * 100}\`;
      const res = await fetch(url);
      const data = await res.json();
      document.getElementById('token-count').textContent = \`Total: \${data.count}\`;
      const tbody = document.getElementById('tokens-body');
      tbody.innerHTML = '';
      data.tokens.forEach(t => {
        tbody.innerHTML += \`<tr><td>\${t.app_id}</td><td>\${t.token.slice(0, 20)}...</td><td>\${t.type}</td><td>\${t.reason}</td><td>\${new Date(t.detected_at).toLocaleString()}</td></tr>\`;
      });
    }

    async function clearInvalidTokens() {
      if (confirm('Clear all invalid tokens?')) {
        const res = await fetch('/dashboard/api/invalid-tokens', { method: 'DELETE' });
        const data = await res.json();
        alert(\`Cleared \${data.deleted} tokens\`);
        loadInvalidTokens();
      }
    }

    async function testSend(e) {
      e.preventDefault();
      const resultDiv = document.getElementById('test-result');
      resultDiv.textContent = 'Sending...';

      try {
        const request = JSON.parse(document.getElementById('test-request').value);
        const res = await fetch('/dashboard/api/test-send', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            app_id: document.getElementById('test-app-id').value,
            token: document.getElementById('test-token').value,
            type: document.getElementById('test-type').value,
            priority: document.getElementById('test-priority').value,
            request
          })
        });
        const data = await res.json();
        resultDiv.innerHTML = \`<span style="color: #4eff4a;">\${data.message}</span>\`;
      } catch (err) {
        resultDiv.innerHTML = \`<span style="color: #ff4a4a;">Error: \${err.message}</span>\`;
      }
    }

    async function loadAppIds() {
      const res = await fetch('/health');
      // Apps would be exposed via a separate endpoint, using config for now
      const select = document.getElementById('test-app-id');
      const apps = ['my_ios_app', 'my_android_app'];
      apps.forEach(app => {
        const opt = document.createElement('option');
        opt.value = app;
        opt.textContent = app;
        select.appendChild(opt);
      });
    }

    loadStats();
    loadQueueStats();
    loadInvalidTokens();
    loadAppIds();
    setInterval(loadStats, 30000);
    setInterval(loadQueueStats, 10000);
  </script>
</body>
</html>`;

module.exports = {
  DASHBOARD_HTML
};
