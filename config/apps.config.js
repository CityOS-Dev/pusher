// config/apps.config.js
module.exports = {
  apps: [
    {
      id: 'my_ios_app',
      type: 'ios',
      cert: './certs/ios_prod.pem',
      passphrase: '',
      production: true,
    },
    {
      id: 'my_android_app',
      type: 'android',
      serviceAccount: './certs/firebase_key.json',
    },
    {
      id: 'sb_ios',
      type: 'ios',
      cert: './certs/nwhome_voip_cert.pem',
      passphrase: '',
      production: true,
    },
    {
      id: 'sb_android',
      type: 'android',
      serviceAccount: './certs/firebase_credentials.json',
    },
    {
      id: 'sb_android_nw',
      type: 'android',
      serviceAccount: './certs/firebase_credentials_nw_service.json',
    },
  ],
}
