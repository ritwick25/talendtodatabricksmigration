import { createApp, analytics, files, server } from '@databricks/appkit';

createApp({
  plugins: [
    analytics(),
    files(),
    server(),
  ],
}).catch(console.error);
