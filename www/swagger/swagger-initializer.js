window.onload = function() {
  //<editor-fold desc="Changeable Configuration Block">

  // Kudu REST API Swagger UI configuration
  window.ui = SwaggerUIBundle({
    url: "./kudu-api.json",
    dom_id: '#swagger-ui',
    deepLinking: true,
    presets: [
      SwaggerUIBundle.presets.apis,
      SwaggerUIStandalonePreset
    ],
    plugins: [
      SwaggerUIBundle.plugins.DownloadUrl
    ],
    layout: "StandaloneLayout"
  });

  //</editor-fold>
};
