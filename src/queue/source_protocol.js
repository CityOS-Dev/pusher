// Shared strict identity grammar for both HTTP validation and status RPC.
// Use one explicit absolute-end assertion in both schema and runtime checks.
const ISSUER_PATTERN = '^asterisk:[A-Za-z0-9_.-]{1,119}(?![\\s\\S])'
const SESSION_PATTERN = '^[A-Za-z0-9_.:-]{1,128}(?![\\s\\S])'
const issuer = new RegExp(ISSUER_PATTERN), session = new RegExp(SESSION_PATTERN)
const validSession = value => typeof value === 'string' && session.test(value)
const validSource = value => value?.kind === 'sip' && typeof value.issuer === 'string' && issuer.test(value.issuer) && validSession(value.session_id)
module.exports = { ISSUER_PATTERN, SESSION_PATTERN, validSession, validSource }
