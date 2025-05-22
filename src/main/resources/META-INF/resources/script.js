async function makeApiCall(method, endpoint, body = null, isFile = false) {
    const baseUrl = document.getElementById('baseUrlInput').value;
    const url = `${baseUrl}${endpoint}`;
    const responseArea = document.getElementById('responseArea');

    // Reset response area state and set to loading
    responseArea.textContent = 'Loading...';
    responseArea.classList.remove('success', 'error');
    responseArea.classList.add('loading');

    const headers = {};
    // Set Content-Type for JSON, unless it's FormData or a File stream
    if (!isFile && body && !(body instanceof FormData)) {
        headers['Content-Type'] = 'application/json';
    }

    try {
        let options = { method, headers };
        if (body) {
            if (body instanceof FormData) { // For multipart file uploads (e.g., distributed load with FormData)
                options.body = body;
                // Content-Type is set by browser with boundary, so remove from our headers if previously set
                delete headers['Content-Type'];
            } else if (isFile && body instanceof File) { // For binary stream uploads
                headers['Content-Type'] = 'application/octet-stream';
                options.body = body;
            } else { // For JSON body
                options.body = JSON.stringify(body);
            }
        }

        const response = await fetch(url, options);
        const rawResponseText = await response.text(); // Read response as text first
        let responseData;

        responseArea.classList.remove('loading'); // Remove loading before processing response

        if (response.ok) {
            responseArea.classList.remove('error'); // Ensure error class is removed on success
            responseArea.classList.add('success');
            try {
                responseData = JSON.parse(rawResponseText); // Attempt to parse as JSON
                // If successful, stringify for pretty display
                responseArea.textContent = `Status: ${response.status}\n\nBody:\n${JSON.stringify(responseData, null, 2)}`;
            } catch (e) {
                // Not JSON or failed to parse, display as raw text
                responseData = rawResponseText;
                responseArea.textContent = `Status: ${response.status}\n\nBody:\n${responseData}`;
            }
        } else { // Not response.ok (API error)
            responseArea.classList.remove('success'); // Ensure success class is removed on error
            responseArea.classList.add('error');
            try {
                // Error responses might also be JSON
                responseData = JSON.parse(rawResponseText);
                responseArea.textContent = `API Error - Status: ${response.status}\n\nBody:\n${JSON.stringify(responseData, null, 2)}`;
            } catch (e) {
                responseData = rawResponseText;
                responseArea.textContent = `API Error - Status: ${response.status}\n\nBody:\n${responseData}`;
            }
            console.error('API Error:', response.status, responseData);
        }
    } catch (error) { // Network or other fetch-related error
        responseArea.classList.remove('loading', 'success');
        responseArea.classList.add('error');
        responseArea.textContent = `Network or Fetch Error: ${error.message}\n\nPlease check the API server URL and your network connection.`;
        console.error('Fetch Error:', error);
    }
    // Scroll to the response area card after updating its content
    document.getElementById('responseAreaCard').scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function listTables() {
    makeApiCall('GET', '/tables');
}

function createTable() {
    const tableName = document.getElementById('createTableNameInput').value;
    const columnsJson = document.getElementById('columnsJsonInput').value;
    const responseArea = document.getElementById('responseArea');
    try {
        const columns = JSON.parse(columnsJson);
        makeApiCall('POST', '/tables', { name: tableName, columns });
    } catch (e) {
        responseArea.classList.remove('success', 'loading');
        responseArea.classList.add('error');
        responseArea.textContent = 'Client Error: Invalid JSON for columns. ' + e.message;
        console.error("JSON Parse Error (Columns):", e);
    }
}

async function loadParquetBinary() {
    const tableName = document.getElementById('loadBinaryTableNameInput').value;
    const fileInput = document.getElementById('parquetFileBinaryInput');
    const rowLimit = document.getElementById('loadBinaryRowLimitInput').value || '0';
    const batchSize = document.getElementById('loadBinaryBatchSizeInput').value || '0';
    const skipRows = document.getElementById('loadBinarySkipRowsInput').value || '0';
    const responseArea = document.getElementById('responseArea');

    if (fileInput.files.length === 0) {
        responseArea.classList.remove('success', 'loading');
        responseArea.classList.add('error');
        responseArea.textContent = 'Client Error: No file selected for binary load.';
        return;
    }
    const file = fileInput.files[0];
    const endpoint = `/tables/${tableName}/load-binary?rowLimit=${rowLimit}&batchSize=${batchSize}&skipRows=${skipRows}`;
    makeApiCall('POST', endpoint, file, true);
}

async function loadParquetDistributed() {
    const tableName = document.getElementById('loadDistributedTableNameInput').value;
    const fileInput = document.getElementById('parquetFileDistributedInput');
    // These parameters are sent as FormData parts, not query params
    const rowLimit = document.getElementById('loadDistributedRowLimitInput').value || '0'; 
    const batchSize = document.getElementById('loadDistributedBatchSizeInput').value || '0';
    const skipRows = document.getElementById('loadDistributedSkipRowsInput').value || '0';
    const responseArea = document.getElementById('responseArea');

    if (fileInput.files.length === 0) {
        responseArea.classList.remove('success', 'loading');
        responseArea.classList.add('error');
        responseArea.textContent = 'Client Error: No file selected for distributed load.';
        return;
    }

    const formData = new FormData();
    formData.append('file', fileInput.files[0]);
    formData.append('rowLimit', rowLimit);
    formData.append('batchSize', batchSize);
    formData.append('skipRows', skipRows);

    const endpoint = `/tables/${tableName}/load-distributed-upload`;
    makeApiCall('POST', endpoint, formData, true); // isFile=true implies body is FormData or File, makeApiCall handles it
}

function getTableStats() {
    const tableName = document.getElementById('statsTableNameInput').value;
    if (!tableName) {
        const responseArea = document.getElementById('responseArea');
        responseArea.classList.remove('success', 'loading');
        responseArea.classList.add('error');
        responseArea.textContent = 'Client Error: Table name is required for stats.';
        return;
    }
    makeApiCall('GET', `/tables/${tableName}/stats`);
}

function executeQuery() {
    const queryDtoJson = document.getElementById('queryDtoJsonInput').value;
    const responseArea = document.getElementById('responseArea');
    try {
        const queryDto = JSON.parse(queryDtoJson);
        makeApiCall('POST', '/query', queryDto);
    } catch (e) {
        responseArea.classList.remove('success', 'loading');
        responseArea.classList.add('error');
        responseArea.textContent = 'Client Error: Invalid JSON for Query DTO. ' + e.message;
        console.error("JSON Parse Error (Query DTO):", e);
    }
}
