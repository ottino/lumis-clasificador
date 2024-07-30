const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const sqlite3 = require('sqlite3');
const pdfParse = require('pdf-parse');
const axios = require('axios');
const natural = require('natural');

// Configuración
const configPath = path.join(__dirname, 'config.json');
let config;

try {
    const configData = fs.readFileSync(configPath, 'utf8');
    config = JSON.parse(configData);
} catch (error) {
    console.error('Error al leer el archivo config.json:', error);
    process.exit(1);
}

// Preprocesamiento de texto
const tokenizer = new natural.WordTokenizer();
const stopwords = new Set(['el', 'la', 'los', 'las', 'un', 'una', 'y', 'o', /* más stopwords */]);

function preprocessText(text, fileName) {
    const tokens = tokenizer.tokenize((fileName + ' ' + text).toLowerCase());
    return tokens.filter(token => !stopwords.has(token)).join(' ');
}

// Funciones auxiliares
function formatDate(date) {
    return date.toISOString().replace('T', ' ').substr(0, 19);
}

function formatFileSize(bytes) {
    if (bytes < 1024) return bytes + ' bytes';
    else if (bytes < 1048576) return (bytes / 1024).toFixed(2) + ' KB';
    else if (bytes < 1073741824) return (bytes / 1048576).toFixed(2) + ' MB';
    else return (bytes / 1073741824).toFixed(2) + ' GB';
}

function generateHash(fileInfo) {
    const data = `${fileInfo.nombre}${fileInfo.fechaCreacion}${fileInfo.fechaModificacion}${fileInfo.tamaño}`;
    return crypto.createHash('md5').update(data).digest('hex');
}

// Función para calcular la similitud del coseno
function cosineSimilarity(vec1, vec2) {
    const dotProduct = vec1.reduce((sum, val, i) => sum + val * vec2[i], 0);
    const magnitude1 = Math.sqrt(vec1.reduce((sum, val) => sum + val * val, 0));
    const magnitude2 = Math.sqrt(vec2.reduce((sum, val) => sum + val * val, 0));
    return dotProduct / (magnitude1 * magnitude2);
}

// Función para buscar documentos similares
async function findSimilarDocuments(db, queryEmbedding, limit = 5) {
    return new Promise((resolve, reject) => {
        db.all('SELECT nombre, embedding FROM file_info', [], (err, rows) => {
            if (err) {
                reject(err);
                return;
            }
            const similarities = rows.map(row => ({
                nombre: row.nombre,
                similarity: cosineSimilarity(queryEmbedding, JSON.parse(row.embedding))
            }));
            similarities.sort((a, b) => b.similarity - a.similarity);
            resolve(similarities.slice(0, limit));
        });
    });
}

// Funciones de base de datos
function createTable(db) {
    const sql = `CREATE TABLE IF NOT EXISTS file_info (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT,
        tipo TEXT,
        fechaCreacion TEXT,
        fechaModificacion TEXT,
        tamaño TEXT,
        hash TEXT,
        chunk_index INTEGER,
        embedding TEXT,
        original_content TEXT
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

async function insertOrUpdateChunks(db, fileInfo, chunks, embeddings) {
    const sql = `INSERT OR REPLACE INTO file_info 
                 (nombre, tipo, fechaCreacion, fechaModificacion, tamaño, hash, chunk_index, embedding, original_content) 
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`;

    for (let i = 0; i < chunks.length; i++) {
        await new Promise((resolve, reject) => {
            db.run(sql, [
                fileInfo.nombre,
                fileInfo.tipo,
                fileInfo.fechaCreacion,
                fileInfo.fechaModificacion,
                fileInfo.tamaño,
                fileInfo.hash,
                i,
                JSON.stringify(embeddings[i]),
                chunks[i]
            ], (err) => {
                if (err) reject(err);
                else resolve();
            });
        });
    }
}

// Funciones de procesamiento de archivos
async function extractTextFromPDF(filePath) {
    const dataBuffer = fs.readFileSync(filePath);
    const data = await pdfParse(dataBuffer);
    return data.text;
}

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

async function generateEmbeddingWithRetry(text, maxRetries = 3) {
    for (let i = 0; i < maxRetries; i++) {
        try {
            return await generateEmbedding(text);
        } catch (error) {
            console.error(`Intento ${i + 1} fallido:`, error);
            if (i === maxRetries - 1) throw error;
            await new Promise(resolve => setTimeout(resolve, 1000 * (i + 1)));
        }
    }
}

function chunkText(text, maxLength = 1000) {
    const chunks = [];
    for (let i = 0; i < text.length; i += maxLength) {
        chunks.push(text.slice(i, i + maxLength));
    }
    return chunks;
}

function normalizeEmbedding(embedding) {
    const magnitude = Math.sqrt(embedding.reduce((sum, val) => sum + val * val, 0));
    return embedding.map(val => val / magnitude);
}

// Función principal de escaneo
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
                    };

                    fileInfo['hash'] = generateHash(fileInfo);

                    console.log('Documento encontrado:', JSON.stringify(fileInfo, null, 2));

                    const row = await new Promise((resolve, reject) => {
                        db.get(`SELECT hash FROM file_info WHERE nombre = ? LIMIT 1`, [fileInfo.nombre], (err, row) => {
                            if (err) {
                                console.error('Error al consultar registro:', err);
                                reject(err);
                            } else {
                                resolve(row);
                            }
                        });
                    });

                    if (!row || row.hash !== fileInfo.hash) {
                        if (fileExt === 'pdf' || fileExt === 'txt' || fileExt === 'sql') {
                            try {
                                const text = fileExt === 'pdf' ? await extractTextFromPDF(filePath) : fs.readFileSync(filePath, 'utf8');
                                const preprocessedText = preprocessText(text, fileInfo.nombre);
                                const chunks = chunkText(preprocessedText);
                                const embeddings = await Promise.all(chunks.map(chunk => generateEmbeddingWithRetry(chunk)));
                                const normalizedEmbeddings = embeddings.map(normalizeEmbedding);
                                await insertOrUpdateChunks(db, fileInfo, chunks, normalizedEmbeddings);
                                
                                console.log(`Embeddings generados y guardados para el archivo ${file}.`);
                            } catch (error) {
                                console.error(`Error al procesar el archivo ${file}:`, error);
                            }
                        } else {
                            await insertOrUpdateChunks(db, fileInfo, [''], [[]]);
                            console.log('Registro insertado.');
                        }
                    } else {
                        console.log('Registro sin cambios.');
                    }

                    console.log('---');
                }
            }
        }
    } catch (error) {
        console.error('Error al escanear el directorio:', dirPath, error);
    }
}

// Función principal
(async () => {
    const db = new sqlite3.Database('file_infov2.db');

    try {
        await createTable(db);

        for (const dirPath of config.monitoreo.paths) {
            console.log(`Escaneando directorio: ${dirPath}`);
            await scanDirectory(db, dirPath, config.monitoreo.extensiones);
        }

        // Ejemplo de búsqueda de documentos similares
        // const queryText = "Texto de ejemplo para buscar documentos similares";
        // const queryEmbedding = await generateEmbedding(queryText);
        // const similarDocuments = await findSimilarDocuments(db, queryEmbedding);
        // console.log('Documentos similares:', similarDocuments);

    } catch (error) {
        console.error('Error en el proceso principal:', error);
    } finally {
        db.close((err) => {
            if (err) {
                console.error('Error al cerrar la base de datos:', err);
            } else {
                console.log('Conexión a la base de datos cerrada.');
            }
        });
    }
})();