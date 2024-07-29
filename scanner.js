const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const sqlite3 = require('sqlite3');
const pdfParse = require('pdf-parse');
const axios = require('axios');

// Leer el archivo config.json
const configPath = path.join(__dirname, 'config.json');
let config;

try {
    const configData = fs.readFileSync(configPath, 'utf8');
    config = JSON.parse(configData);
} catch (error) {
    console.error('Error al leer el archivo config.json:', error);
    process.exit(1);
}

// Función para formatear fechas
function formatDate(date) {
    return date.toISOString().replace('T', ' ').substr(0, 19);
}

// Función para formatear el tamaño del archivo
function formatFileSize(bytes) {
    if (bytes < 1024) return bytes + ' bytes';
    else if (bytes < 1048576) return (bytes / 1024).toFixed(2) + ' KB';
    else if (bytes < 1073741824) return (bytes / 1048576).toFixed(2) + ' MB';
    else return (bytes / 1073741824).toFixed(2) + ' GB';
}

// Función para generar un hash único
function generateHash(fileInfo) {
    const data = `${fileInfo.nombre}${fileInfo.fechaCreacion}${fileInfo.fechaModificacion}${fileInfo.tamaño}`;
    return crypto.createHash('md5').update(data).digest('hex');
}

// Función para crear la tabla en SQLite3 si no existe
function createTable(db) {
    const sql = `CREATE TABLE IF NOT EXISTS file_info (
        nombre TEXT PRIMARY KEY,
        tipo TEXT,
        fechaCreacion TEXT,
        fechaModificacion TEXT,
        tamaño TEXT,
        hash TEXT,
        embedding TEXT,
        original_content TEXT  -- Campo para el contenido original
    );`;

    return new Promise((resolve, reject) => {
        db.run(sql, (err) => {
            if (err) {
                console.error('Error al crear la tabla:', err);
                reject(err);
            } else {
                resolve();
            }
        });
    });
}

// Función para insertar o actualizar un registro en SQLite3
function insertOrUpdate(db, fileInfo) {
    // console.log("holaaaa!!! ***");
    // console.log(fileInfo.original_content);
    const sql = `INSERT OR REPLACE INTO file_info (nombre, tipo, fechaCreacion, fechaModificacion, tamaño, hash, embedding, original_content) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`;

    return new Promise((resolve, reject) => {
        db.run(sql, [fileInfo.nombre, fileInfo.tipo, fileInfo.fechaCreacion, fileInfo.fechaModificacion, fileInfo.tamaño, fileInfo.hash, fileInfo.embedding, fileInfo.original_content], (err) => {
            if (err) {
                console.error('Error al insertar o actualizar registro:', err);
                reject(err);
            } else {
                resolve();
            }
        });
    });
}

// Función para extraer texto de un archivo PDF
async function extractTextFromPDF(filePath) {
    const dataBuffer = fs.readFileSync(filePath);
    const data = await pdfParse(dataBuffer);
    return data.text;
}

// Función para generar embeddings usando la API de OLLAMA
async function generateEmbedding(text) {
    try {
        const response = await axios.post('http://127.0.0.1:11434/api/embeddings', {
            model: 'mxbai-embed-large',
            prompt: text
        });
        return response.data.embedding;
    } catch (error) {
        console.error('Error generating embedding:', error);
        throw error;
    }
}

// Función para escanear un directorio y procesar archivos
async function scanDirectory(db, dirPath, extensions) {
    try {
        const files = fs.readdirSync(dirPath);

        for (const file of files) {
            const filePath = path.join(dirPath, file);
            const stat = fs.statSync(filePath);

            if (stat.isDirectory()) {
                await scanDirectory(db, filePath, extensions);
            } else {
                const fileExt = path.extname(file).toLowerCase().slice(1);
                if (extensions.includes(fileExt)) {
                    const fileInfo = {
                        nombre: file,
                        tipo: fileExt.toUpperCase(),
                        fechaCreacion: formatDate(stat.birthtime),
                        fechaModificacion: formatDate(stat.mtime),
                        tamaño: formatFileSize(stat.size),
                        embedding: null,
                        original_content: null // Inicializar el campo para el contenido original
                    };

                    fileInfo['hash'] = generateHash(fileInfo);

                    console.log('Documento encontrado:');
                    console.log(JSON.stringify(fileInfo, null, 2));

                    const row = await new Promise((resolve, reject) => {
                        db.get(`SELECT hash FROM file_info WHERE nombre = ?`, [fileInfo.nombre], (err, row) => {
                            if (err) {
                                console.error('Error al consultar registro:', err);
                                reject(err);
                            } else {
                                resolve(row);
                            }
                        });
                    });

                    if (!row || row.hash !== fileInfo.hash) {
                        // Si el archivo es nuevo o si el hash ha cambiado
                        if (fileExt === 'pdf') {
                            try {
                                const text = await extractTextFromPDF(filePath);
                                const txtFilePath = path.join(dirPath, path.basename(file, path.extname(file)) + '.txt');
                                fs.writeFileSync(txtFilePath, text, 'utf8');
                                console.log(`Texto extraído y guardado en: ${txtFilePath}`);

                                const embedding = await generateEmbedding(text);
                                fileInfo.embedding = JSON.stringify(embedding);
                                fileInfo.original_content = text; // Guardar el contenido original
                                await insertOrUpdate(db, fileInfo);
                                console.log('Embedding generado y guardado en la base de datos.');
                            } catch (error) {
                                console.error(`Error al extraer texto del PDF ${file}:`, error);
                            }
                        } else if (fileExt === 'txt' || fileExt === 'sql') {
                            try {
                                const text = fs.readFileSync(filePath, 'utf8');
                                fileInfo.original_content = text; // Guardar el contenido original
                                const embedding = await generateEmbedding(text);
                                fileInfo.embedding = JSON.stringify(embedding);
                                await insertOrUpdate(db, fileInfo);
                                console.log(`Embedding generado para el archivo ${file} y guardado en la base de datos.`);
                            } catch (error) {
                                console.error(`Error al leer el archivo ${file}:`, error);
                            }
                        } else {
                            await insertOrUpdate(db, fileInfo);
                            console.log('Registro insertado.');
                        }
                    } else {
                        console.log('Registro sin cambios.');
                    }

                    console.log(row ? `${row.hash} - ${fileInfo.hash}` : `No hay fila - ${fileInfo.hash}`);
                    console.log('---');
                }
            }
        }
    } catch (error) {
        console.error('Error al escanear el directorio:', dirPath, error);
    }
}

// Conectar a la base de datos SQLite3
const db = new sqlite3.Database('file_info.db', (err) => {
    if (err) {
        console.error('Error al conectar a la base de datos:', err);
    } else {
        console.log('Conexión a la base de datos establecida.');
    }
});

// Crear la tabla si no existe
(async () => {
    await createTable(db);

    // Recorrer cada path definido en el config y esperar que todos los escaneos terminen antes de cerrar la base de datos
    for (const dirPath of config.monitoreo.paths) {
        console.log(`Escaneando directorio: ${dirPath}`);
        await scanDirectory(db, dirPath, config.monitoreo.extensiones);
    }

    db.close((err) => {
        if (err) {
            console.error('Error al cerrar la base de datos:', err);
        } else {
            console.log('Conexión a la base de datos cerrada.');
        }
    });
})();
