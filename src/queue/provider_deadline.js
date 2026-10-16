// Transport operation budget; this is not a universal call lifetime.
async function sendWithin(send, milliseconds) {
  let timer
  try {
    return await Promise.race([Promise.resolve().then(send), new Promise((_, reject) => {
      timer = setTimeout(() => reject(Object.assign(new Error('Provider result unknown after transport timeout'), { code:'ETIMEDOUT' })), milliseconds)
    })])
  } finally { clearTimeout(timer) }
}
module.exports = { sendWithin }
