<html>

<head>
    <meta charset="utf-8" />
    <title>TiDB Playground</title>
    <link rel="icon" href="/favicon.ico" type="image/x-icon">
    <script type="text/javascript" src="https://code.jquery.com/jquery-2.1.1.min.js"></script>
    <script type="text/javascript" src="jquery.console.js"></script>
    <script src="wasm_exec.js"></script>
    <style type="text/css" media="screen">
    div.term {
        font-size: 14px;
        margin-top: 1em
    }

    div.term div.jquery-console-inner {
        height: 800px;
        background: black;
        padding: 0.5em;
        overflow: auto
    }

    div.term div.jquery-console-prompt-box {
        color: white;
        font-family: monospace;
    }

    div.term div.jquery-console-focus span.jquery-console-cursor {
        background: #444;
        color: #eee;
        font-weight: bold
    }

    div.term div.jquery-console-message-error {
        color: #ef0505;
        font-family: sans-serif;
        font-weight: bold;
        padding: 0.1em;
    }

    div.term div.jquery-console-message-success {
        color: #09f753;
        font-family: monospace;
        padding: 0.1em;
        white-space: pre;
    }

    div.term span.jquery-console-prompt-label {
        font-weight: bold;
    }

    /* .inner {
        display: table;
        margin: 0 auto;
        font-family: monospace;
        font-size: 20px;
    }

    .outer {
        width: 100%
    } */
    body {
        min-height: 100vh;
        min-width: 100vw;
        background-color: #000000;
    }

    .loading-wrap {
        display: flex;
        align-items: center;
        justify-content: center;
        min-height: 100vh;
        min-width: 100vw;
        font-family: "Poppins", sans-serif;
        background-color: #000000;
    }

    .loading {
        position: relative;
        padding: 0.1em;
        font-size: 8vw;
        line-height: 1.2;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: #222222;
        border-bottom: 0.12em solid;
    }

    .loading-before {
        position: absolute;
        top: 0;
        left: 0;
        padding: inherit;
        overflow: hidden;
        color: green;
        text-shadow: 0 0 0.2em;
        border-bottom: inherit;
        box-sizing: border-box;
        width: 0%;
    }

    .animated {
        animation: textAnim 2s infinite alternate linear;
    }

    @keyframes textAnim {
        0% {
            width: 0;
        }

        100% {
            width: 100%;
        }
    }
    </style>
</head>

<body>
    <script>
    function unimplemented(callback) {
        const err = new Error("not implemented");
        err.code = "ENOSYS";
        callback(err);
    }
    function unimplemented1(_1, callback) { unimplemented(callback); }
    function unimplemented2(_1, _2, callback) { unimplemented(callback); }
    function bootstrapGo() {
        const go = new Go();
        fetch("main.css")
            .then(progressHandler)
            .then(r => r.arrayBuffer())
            .then(buffer => WebAssembly.instantiate(buffer, go.importObject))
            .then(result => {
                fs.stat = unimplemented1;
                fs.lstat = unimplemented1;
                fs.unlink = unimplemented1;
                fs.rmdir = unimplemented1;
                fs.mkdir = unimplemented2;
                go.run(result.instance);
                document.getElementById("loading").style.display = "none";
                bootstrapTerm();
            });
    }

    function bootstrapTerm() {
        window.term = $('<div class="term">');
        $('body').append(term);
        window.shell = term.console({
            promptLabel: 'TiDB> ',
            continuedPromptLabel: ' -> ',
            commandValidate: function(line) {
                if (line == "") {
                    return false;
                } else {
                    return true;
                }
            },
            commandHandle: function(line, report) {
                if (line.trim().endsWith(';')) {
                    window.shell.continuedPrompt = false;
                    executeSQL(line, function(msg) {
                        report([{
                            msg,
                            className: "jquery-console-message-success"
                        }])
                    })
                } else {
                    window.shell.continuedPrompt = true;
                }
            },
            promptHistory: true
        });
        window.upload = function(onsuccess, onerror) {
            const uploader = document.getElementById('file-uploader')
            uploader.addEventListener('change', function() {
                const reader = new FileReader();
                reader.addEventListener('load', function(e) {
                    onsuccess(e.target.result);
                });
                reader.addEventListener('error', function() {
                    onerror("failed to read file")
                });
                if (this.files.length == 0) {
                    onerror("no file selected")
                } else {
                    reader.readAsText(this.files[0]);
                }
            });
            uploader.click();
        };
    }

    bootstrapGo();

    function progress({
        loaded,
        total
    }) {
        const num = Math.round(loaded / total * 100) + '%'
        $('.loading-before').css({
            'width': num
        })
        if (num == '100%') {
            $('.loading').html('<div class="loading-before">Running</div>Running')
            $('.loading-before').addClass('animated')
        }
    }

    function progressHandler(response) {
        if (!response.ok) {
            throw Error(response.status + ' ' + response.statusText)
        }

        if (!response.body) {
            throw Error('ReadableStream not yet supported in this browser.')
        }

        // hardcode since some CDN does NOT return content-length for compressed content
        const total = 82837504 
        let loaded = 0;

        return new Response(
            new ReadableStream({
                start(controller) {
                    const reader = response.body.getReader();

                    read();

                    function read() {
                        reader.read().then(({
                            done,
                            value
                        }) => {
                            if (done) {
                                controller.close();
                                return;
                            }
                            loaded += value.byteLength;
                            progress({
                                loaded,
                                total
                            })
                            controller.enqueue(value);
                            read();
                        }).catch(error => {
                            console.error(error);
                            controller.error(error)
                        })
                    }
                }
            })
        );
    }
    </script>
    <input id='file-uploader' type='file' hidden />
    <div class="loading-wrap" id='loading'>
        <h1 class="loading"><div class="loading-before">Loading…</div>Loading…</h1>
    </div>
</body>

</html>
