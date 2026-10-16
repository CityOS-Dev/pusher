// Transport operation budget; this is not a universal call lifetime.
async function sendWithin(send, milliseconds) {
  let timer
  try {
    return await Promise.race([Promise.resolve().then(send), new Promise((_, reject) => {
      timer = setTimeout(() => reject(Object.assign(new Error('Provider result unknown after transport timeout'), { code:'ETIMEDOUT' })), milliseconds)
    })])
  } finally { clearTimeout(timer) }
}

// Public APNs/FCM send APIs cannot cancel an individual SDK submission. Keep
// timed-out work counted until it actually settles, rather than spawning an
// unbounded number of hidden requests as Bull releases its worker slots.
function createBudget(limit) {
  let pending = 0
  return {
    acquire() {
      if (pending >= limit) return null
      pending++
      let started = false, released = false
      const settle = () => {
        if (!released) { released = true; pending-- }
      }
      return {
        release() { if (!started) settle() },
        send(send, milliseconds) {
          if (started || released) throw new Error('Provider lease already used')
          started = true
          const operation = Promise.resolve().then(send)
          operation.then(settle, settle)
          return sendWithin(() => operation, milliseconds)
        },
      }
    },
    health() { return { pending, limit, saturated: pending >= limit } },
  }
}

const providerBudget = createBudget(4) // Two workers in each of two queues.
module.exports = { sendWithin, createBudget, providerBudget }
