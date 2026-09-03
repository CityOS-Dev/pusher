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
    .btn { padding: 8px 16px; background: #4a9eff; color: #ffffff; border: none; border-radius: 4px; cursor: pointer; margin-right: 10px; }
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
    .pagination { margin-top: 15px; display: flex; gap: 10px; align-items: center; }
    .page-info { color: #888; }
  </style>
</head>
<body>
  <div class="container">
    <h1>Push Notification Dashboard</h1>

    <div class="section">
      <h2>Queue Management</h2>
      <div style="margin-bottom: 20px;">
        <a href="/dashboard/queues" class="btn" style="text-decoration: none;" target="_blank">Open Bull Board</a>
      </div>
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
      <div class="pagination">
        <button class="btn" onclick="previousPage()">Previous</button>
        <span class="page-info" id="page-info">Page 1</span>
        <button class="btn" onclick="nextPage()">Next</button>
      </div>
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
    "notification": {
      "title": "Test Notification",
      "body": "This is a test"
    }
  }
}</textarea>
        </div>
        <button type="submit" class="btn">Send Test Push</button>
      </form>
      <div id="test-result" style="margin-top: 15px;"></div>
    </div>
    
    <div class="section">
      <h2>Statistics</h2>
      <table id="stats">
        <thead>
          <tr><td>DateTime</td><td>App</td><td>Success</td><td>Failed</td></tr>
        </thead>
        <tbody id="stats-body">
        </tbody>
      </table>
    </div>
  </div>

  <script>
    let currentPage = 0

    async function loadStats() {
      const res = await fetch('/dashboard/api/stats')
      const data = await res.json()
      const container = document.getElementById('stats-body')
      container.innerHTML = ''
      for (const item of data) {
        container.innerHTML += \`
          <tr>
            <td>\${item.hour}</td>
            <td>\${item.app_id}</td>
            <td style="color: #4eff4a;">\${item.sent_success}</td>
            <td style="color: #ff4a4a; font-size: 18px;">\${item.sent_fail}</td>
          </tr>
        \`
      }
    }

    async function loadQueueStats() {
      const res = await fetch('/health')
      const data = await res.json()
      const container = document.getElementById('queue-stats')
      if (!data.queues) {
        container.innerHTML = '<div style="color: #ff4a4a;">Queue stats not available</div>'
        return
      }
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
        <div class="stat-card">
          <div class="stat-label">High Waiting</div>
          <div class="stat-value">\${data.queues.high.waiting}</div>
          <div class="stat-label">Jobs</div>
        </div>
        <div class="stat-card">
          <div class="stat-label">Standard Waiting</div>
          <div class="stat-value">\${data.queues.standard.waiting}</div>
          <div class="stat-label">Jobs</div>
        </div>
      \`
    }

    async function loadInvalidTokens(page = 0) {
      currentPage = page
      const appId = document.getElementById('appIdFilter').value
      const url = appId ? \`/dashboard/api/invalid-tokens?app_id=\${appId}&limit=100&offset=\${page * 100}\` : \`/dashboard/api/invalid-tokens?limit=100&offset=\${page * 100}\`
      const res = await fetch(url)
      const data = await res.json()
      document.getElementById('token-count').textContent = \`Total: \${data.count}\`
      document.getElementById('page-info').textContent = \`Page \${page + 1}\`
      const tbody = document.getElementById('tokens-body')
      tbody.innerHTML = ''
      data.tokens.forEach(t => {
        tbody.innerHTML += \`<tr><td>\${t.app_id}</td><td>\${t.token.slice(0, 20)}...</td><td>\${t.type}</td><td>\${t.reason}</td><td>\${new Date(t.detected_at).toLocaleString()}</td></tr>\`
      })
    }

    async function previousPage() {
      if (currentPage > 0) {
        await loadInvalidTokens(currentPage - 1)
      }
    }

    async function nextPage() {
      await loadInvalidTokens(currentPage + 1)
    }

    async function clearInvalidTokens() {
      if (confirm('Clear all invalid tokens?')) {
        const res = await fetch('/dashboard/api/invalid-tokens', { method: 'DELETE' })
        const data = await res.json()
        alert(\`Cleared \${data.deleted} tokens\`)
        await loadInvalidTokens(currentPage)
      }
    }

    async function testSend(e) {
      e.preventDefault()
      const resultDiv = document.getElementById('test-result')
      resultDiv.textContent = 'Sending...'

      try {
        const request = JSON.parse(document.getElementById('test-request').value)
        const res = await fetch('/dashboard/api/test-send', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            app_id: document.getElementById('test-app-id').value,
            token: document.getElementById('test-token').value,
            type: document.getElementById('test-type').value,
            priority: document.getElementById('test-priority').value,
            request,
          }),
        })
        const data = await res.json()
        resultDiv.innerHTML = \`<span style="color: #4eff4a;">\${data.message}</span>\`
      } catch (err) {
        resultDiv.innerHTML = \`<span style="color: #ff4a4a;">Error: \${err.message}</span>\`
      }
    }

    async function loadAppIds() {
      const select = document.getElementById('test-app-id')
      // Default app IDs (hardcoded for now, can be loaded from config endpoint)
      const apps = ['sb_ios', 'sb_android', 'sb_android_nw', 'my_ios_app', 'my_android_app']
      apps.forEach(app => {
        const opt = document.createElement('option')
        opt.value = app
        opt.textContent = app
        select.appendChild(opt)
      })
    }

    loadStats()
    loadQueueStats()
    loadInvalidTokens()
    loadAppIds()
    setInterval(loadStats, 30000)
    setInterval(loadQueueStats, 10000)
  </script>
</body>
</html>`;

module.exports = {
  DASHBOARD_HTML
};
