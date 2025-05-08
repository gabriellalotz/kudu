// Kudu Swagger UI initialization
console.log('Kudu Swagger init script loaded - v2');

function initializeSwaggerUI() {
  console.log('Initializing Swagger UI...');
  console.log('SwaggerUIBundle available:', typeof SwaggerUIBundle !== 'undefined');
  
  if (typeof SwaggerUIBundle === 'undefined') {
    document.getElementById('swagger-ui').innerHTML = '<div style="color: red; padding: 20px; border: 1px solid red;">ERROR: SwaggerUIBundle is not loaded! Check browser console for details.</div>';
    return;
  }
  
  try {
    // Get base URL from data attribute
    const swaggerContainer = document.getElementById('swagger-ui');
    const baseUrl = swaggerContainer ? swaggerContainer.getAttribute('data-base-url') || '' : '';
    console.log('Base URL:', baseUrl);
    
    // Initialize Swagger UI with BaseLayout (no navbar)
    const ui = SwaggerUIBundle({
      url: baseUrl + '/swagger/kudu-api.json',
      dom_id: '#swagger-ui',
      deepLinking: true,
      presets: [
        SwaggerUIBundle.presets.apis
      ],
      plugins: [
        SwaggerUIBundle.plugins.DownloadUrl
      ],
      layout: "BaseLayout"
    });
    
    console.log('Swagger UI initialized successfully');
  } catch (error) {
    console.error('Error initializing Swagger UI:', error);
    document.getElementById('swagger-ui').innerHTML = '<div style="color: red; padding: 20px; border: 1px solid red;">ERROR: ' + error.message + '</div>';
  }
}

// Initialize when DOM is ready
window.addEventListener('load', initializeSwaggerUI);