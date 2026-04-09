// config/apps.config.js
module.exports = {
  apps: [
    {
      id: "my_ios_app",
      type: "ios",
      cert: "./certs/ios_prod.pem",
      passphrase: "",
      production: true
    },
    {
      id: "my_android_app",
      type: "android",
      serviceAccount: "./certs/firebase_key.json"
    }
  ]
};
