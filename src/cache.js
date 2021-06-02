const util = require("util");
const fs = require("fs");
const path = require("path");
const tar = require("tar-stream");

const {
  mkdirpRecSync
} = require("./utils");

/**
 * walkDirectory"s read callback
 *
 * @callback WalkDirectoryReadCallback
 * @param {number} file
 * @param {import("fs").Stats} stats
 * @param {Function} next callback
 */

/**
 * walk directory
 *
 * @param rootDir root directory
 * @param rootRela relative path
 * @param readCb {WalkDirectoryReadCallback} file callback
 * @return {Promise<void>} promise
 */
function walkDirectory(rootDir, rootRela, readCb) {
  return new Promise((resolve, reject) => {
    const walk = (relaDir, done) => {
      const absDir = path.join(rootDir, relaDir);
      fs.readdir(absDir, function(err, list) {
        if (err) return done(err);
        let i = 0;
        (function next() {
          const file = list[i++];
          if (!file) return done(null);
          const relaFile = path.join(relaDir, file);
          const absFile = path.resolve(absDir, file);
          fs.lstat(absFile, function(err, stat) {
            readCb(relaFile, stat, () => {
              if (stat && stat.isDirectory()) {
                walk(relaFile, function (err) {
                  if (err) console.error(err);
                  next();
                });
              } else {
                next();
              }
            });
          });
        })();
      });
    };
    walk(rootRela, (err) => {
      if (err) reject(err);
      else resolve();
    });
  });
}

function retrieveCacheFiles(cacheFile) {
  return new Promise((resolve, reject) => {
    const files = {};
    const extract = tar.extract();
    extract
      .on("error", (err) => reject(err))
      .on("entry", (headers, stream, next) => {
        files[headers.name] = headers;
        stream
          .on("end", () => next())
          .resume();
      })
      .on("finish", () => {
        resolve(files);
      });
    fs.createReadStream(cacheFile)
      .pipe(extract);
  });
}

/**
 * addFileToTarEntry
 *
 * @param pack
 * @param rootDir
 * @param name
 * @param stat {import("fs").Stats}
 * @return {Promise<unknown>}
 */
function addFileToTarEntry(rootDir, pack, name, stat) {
  return new Promise((resolve, reject) => {
    const absFile = path.join(rootDir, name);
    const headers = {
      name,
      mode: stat.mode,
      mtime: stat.mtime,
      size: stat.size,
      uid: stat.uid,
      gid: stat.gid
    };
    let handler = null;
    if (stat.isFile()) {
      headers.type = "file";
      handler = (entry) => {
        fs.createReadStream(absFile)
          .pipe(entry);
      };
    } else if (stat.isSymbolicLink()) {
      headers.type = "symlink";
      headers.linkname = fs.readlinkSync(absFile);
    } else if (stat.isDirectory()) {
      headers.type = "directory";
    } else {
      reject(new Error("not supported file type"));
      return ;
    }
    const entry = pack.entry(headers, (err) => {
      if (err) reject(err);
      else resolve();
    });
    if (handler) handler (entry);
  });
}

function writeFileByStream(file, opts, stream) {
  return new Promise((resolve, reject) => {
    stream.pipe(fs.createWriteStream(file, opts))
      .on("error", reject)
      .on("finish", resolve);
  });
}

/**
 * extractFileFromTarEntry
 *
 * @param rootDir
 * @param headers {import("tar-stream").Headers}
 * @param stream
 * @return {Promise<void>}
 */
function extractFileFromTarEntry(rootDir, headers, stream) {
  const absFile = path.join(rootDir, headers.name);

  mkdirpRecSync(path.dirname(absFile));

  const pipeline = [];
  if (headers.type === "directory") {
    pipeline.push(() => util.promisify(fs.mkdir)(absFile, { mode: headers.mode }));
  } else if (headers.type === "file") {
    pipeline.push(() => writeFileByStream(absFile, { mode: headers.mode }, stream));
  } else if (headers.type === "symlink") {
    pipeline.push(() => util.promisify(fs.symlink)(headers.linkname, absFile));
  } else {
    return Promise.reject(new Error("not supported file type"));
  }

  // path: PathLike, atime: string | number | Date, mtime: string | number | Date, callback: NoParamCallback
  pipeline.push(() =>
    util.promisify(fs.utimes)(
      absFile,
      new Date(),
      headers.mtime
    )
  );

  if (typeof headers.uid !== "undefined") {
    pipeline.push(() =>
      util.promisify(fs.chown)(absFile, headers.uid, headers.gid)
        .catch((err) => console.error(err))
    );
  }

  return pipeline.reduce((prev, cur) =>
    prev.then(() => cur()),
    Promise.resolve()
  );
}

function consumeDummyStream(stream, next) {
  stream
    .on("end", () => next())
    .resume();
}

async function cacheUpdate(rootDir, sources, cacheFile) {
  const tempFile = cacheFile + ".tmp";
  const cachedFiles = {};
  const pack = tar.pack();

  sources = sources.map(v => path.normalize(v));
  pack.pipe(fs.createWriteStream(tempFile));

  for (const source of sources) {
    const absSource = path.join(rootDir, source);
    if (!fs.existsSync(absSource)) continue;
    const sourceStat = fs.statSync(absSource);

    cachedFiles[source] = sourceStat;
    await addFileToTarEntry(rootDir, pack, source, sourceStat)
      .catch((err) => console.error(err));

    if (sourceStat.isDirectory()) {
      await walkDirectory(rootDir, source, (file, stat, next) => {
        cachedFiles[file] = stat;
        addFileToTarEntry(rootDir, pack, file, stat)
          .catch((err) => console.error(err))
          .finally(() => next());
      });
    }
  }

  if (fs.existsSync(cacheFile)) {
    await new Promise((resolve ,reject) => {
      const extract = tar.extract();
      extract
        .on("error", (err) => reject(err))
        .on("entry", (headers, stream, next) => {
          if (headers.name in cachedFiles) {
            // Already cached
            consumeDummyStream(stream, next);
          } else {
            const entry = pack.entry(headers, (err) => {
              next(err);
            });
            if (headers.type === "file") {
              stream.pipe(entry);
            }
          }
        })
        .on("finish", () => {
          resolve();
        });
      fs.createReadStream(cacheFile)
        .pipe(extract);
    })
      .catch((err) => console.error(err));
    fs.unlinkSync(cacheFile);
  }

  pack.finalize();

  fs.renameSync(tempFile, cacheFile);
}

function pathIsContains(sources, name) {
  for (const source of sources) {
    if (name.startsWith(source)) {
      return true;
    }
  }
  return false;
}

async function cacheExtract(rootDir, sources, cacheFile) {
  if (!fs.existsSync(cacheFile)) {
    return ;
  }

  sources = sources.map(v => path.normalize(v));

  await new Promise((resolve ,reject) => {
    const extract = tar.extract();
    extract
      .on("error", (err) => reject(err))
      .on("entry", (headers, stream, next) => {
        if (pathIsContains(sources, headers.name)) {
          extractFileFromTarEntry(rootDir, headers, stream)
            .finally(() => next());
        } else {
          consumeDummyStream(stream, next);
        }
      })
      .on("finish", () => {
        resolve();
      });
    fs.createReadStream(cacheFile)
      .pipe(extract);
  })
    .catch((err) => console.error(err));
}

module.exports = {
  update: cacheUpdate,
  extract: cacheExtract
};
