// JQuery Console 1.0
// Sun Feb 21 20:28:47 GMT 2010
//
// Copyright 2010 Chris Done, Simon David Pratt. All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
//    1. Redistributions of source code must retain the above
//       copyright notice, this list of conditions and the following
//       disclaimer.
//
//    2. Redistributions in binary form must reproduce the above
//       copyright notice, this list of conditions and the following
//       disclaimer in the documentation and/or other materials
//       provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
// FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
// COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
// INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
// LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
// ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

// TESTED ON
//   Internet Explorer 6
//   Opera 10.01
//   Chromium 4.0.237.0 (Ubuntu build 31094)
//   Firefox 3.5.8, 3.6.2 (Mac)
//   Safari 4.0.5 (6531.22.7) (Mac)
//   Google Chrome 5.0.375.55 (Mac)

(function($) {
    var isWebkit = !!~navigator.userAgent.indexOf(' AppleWebKit/');

    $.fn.console = function(config) {
        ////////////////////////////////////////////////////////////////////////
        // Constants
        // Some are enums, data types, others just for optimisation
        var keyCodes = {
            // left
            37: moveBackward,
            // right
            39: moveForward,
            // up
            38: previousHistory,
            // down
            40: nextHistory,
            // backspace
            8: backDelete,
            // delete
            46: forwardDelete,
            // end
            35: moveToEnd,
            // start
            36: moveToStart,
            // return
            13: commandTrigger,
            // tab
            18: doNothing,
            // tab
            9: doComplete
        };
        var ctrlCodes = {
            // C-a
            65: moveToStart,
            // C-e
            69: moveToEnd,
            // C-d
            68: forwardDelete,
            // C-n
            78: nextHistory,
            // C-p
            80: previousHistory,
            // C-b
            66: moveBackward,
            // C-f
            70: moveForward,
            // C-k
            75: deleteUntilEnd,
            // C-l
            76: clearScreen,
            // C-u
            85: clearCurrentPrompt
        };
        if (config.ctrlCodes) {
            $.extend(ctrlCodes, config.ctrlCodes);
        }
        var altCodes = {
            // M-f
            70: moveToNextWord,
            // M-b
            66: moveToPreviousWord,
            // M-d
            68: deleteNextWord
        };
        var shiftCodes = {
            // return
            13: newLine,
        };
        var cursor = '<span class="jquery-console-cursor">&nbsp;</span>';

        ////////////////////////////////////////////////////////////////////////
        // Globals
        var container = $(this);
        var inner = $('<div class="jquery-console-inner"></div>');
        // erjiang: changed this from a text input to a textarea so we
        // can get pasted newlines
        var typer = $('<textarea autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false" class="jquery-console-typer"></textarea>');
        // Prompt
        var promptBox;
        var prompt;
        var continuedPromptLabel = config && config.continuedPromptLabel ?
            config.continuedPromptLabel : "> ";
        var column = 0;
        var promptText = '';
        var restoreText = '';
        var continuedText = '';
        var fadeOnReset = config.fadeOnReset !== undefined ? config.fadeOnReset : true;
        // Prompt history stack
        var history = [];
        var ringn = 0;
        // For reasons unknown to The Sword of Michael himself, Opera
        // triggers and sends a key character when you hit various
        // keys like PgUp, End, etc. So there is no way of knowing
        // when a user has typed '#' or End. My solution is in the
        // typer.keydown and typer.keypress functions; I use the
        // variable below to ignore the keypress event if the keydown
        // event succeeds.
        var cancelKeyPress = 0;
        // When this value is false, the prompt will not respond to input
        var acceptInput = true;
        // When this value is true, the command has been canceled
        var cancelCommand = false;

        // External exports object
        var extern = {};

        ////////////////////////////////////////////////////////////////////////
        // Main entry point
        (function() {
            extern.promptLabel = config && config.promptLabel ? config.promptLabel : "> ";
            container.append(inner);
            inner.append(typer);
            typer.css({ position: 'absolute', top: 0, left: '-9999px' });
            if (config.welcomeMessage)
                message(config.welcomeMessage, 'jquery-console-welcome');
            newPromptBox();
            if (config.autofocus) {
                inner.addClass('jquery-console-focus');
                typer.focus();
                setTimeout(function() {
                    inner.addClass('jquery-console-focus');
                    typer.focus();
                }, 100);
            }
            extern.inner = inner;
            extern.typer = typer;
            extern.scrollToBottom = scrollToBottom;
            extern.report = report;
            extern.showCompletion = showCompletion;
            extern.clearScreen = clearScreen;
        })();

        ////////////////////////////////////////////////////////////////////////
        // Reset terminal
        extern.reset = function() {
            var welcome = (typeof config.welcomeMessage != 'undefined');

            var removeElements = function() {
                inner.find('div').each(function() {
                    if (!welcome) {
                        $(this).remove();
                    } else {
                        welcome = false;
                    }
                });
            };

            if (fadeOnReset) {
                inner.parent().fadeOut(function() {
                    removeElements();
                    newPromptBox();
                    inner.parent().fadeIn(focusConsole);
                });
            } else {
                removeElements();
                newPromptBox();
                focusConsole();
            }
        };

        var focusConsole = function() {
            inner.addClass('jquery-console-focus');
            typer.focus();
        };

        extern.focus = function() {
            focusConsole();
        }

        ////////////////////////////////////////////////////////////////////////
        // Reset terminal
        extern.notice = function(msg, style) {
            var n = $('<div class="notice"></div>').append($('<div></div>').text(msg))
                .css({ visibility: 'hidden' });
            container.append(n);
            var focused = true;
            if (style == 'fadeout')
                setTimeout(function() {
                    n.fadeOut(function() {
                        n.remove();
                    });
                }, 4000);
            else if (style == 'prompt') {
                var a = $('<br/><div class="action"><a href="javascript:">OK</a><div class="clear"></div></div>');
                n.append(a);
                focused = false;
                a.click(function() {
                    n.fadeOut(function() {
                        n.remove();
                        inner.css({ opacity: 1 })
                    });
                });
            }
            var h = n.height();
            n.css({ height: '0px', visibility: 'visible' })
                .animate({ height: h + 'px' }, function() {
                    if (!focused) inner.css({ opacity: 0.5 });
                });
            n.css('cursor', 'default');
            return n;
        };

        ////////////////////////////////////////////////////////////////////////
        // Make a new prompt box
        function newPromptBox() {
            column = 0;
            promptText = '';
            ringn = 0; // Reset the position of the history ring
            enableInput();
            promptBox = $('<div class="jquery-console-prompt-box"></div>');
            var label = $('<span class="jquery-console-prompt-label"></span>');
            var labelText = extern.continuedPrompt ? continuedPromptLabel : extern.promptLabel;
            promptBox.append(label.text(labelText).show());
            label.html(label.html().replace(' ', '&nbsp;'));
            prompt = $('<span class="jquery-console-prompt"></span>');
            promptBox.append(prompt);
            inner.append(promptBox);
            updatePromptDisplay();
        };

        ////////////////////////////////////////////////////////////////////////
        // Handle setting focus
        container.click(function() {
            // Don't mess with the focus if there is an active selection
            if (window.getSelection().toString()) {
                return false;
            }

            inner.addClass('jquery-console-focus');
            inner.removeClass('jquery-console-nofocus');
            if (isWebkit) {
                typer.focusWithoutScrolling();
            } else {
                typer.css('position', 'fixed').focus();
            }
            scrollToBottom();
            return false;
        });

        ////////////////////////////////////////////////////////////////////////
        // Handle losing focus
        typer.blur(function() {
            inner.removeClass('jquery-console-focus');
            inner.addClass('jquery-console-nofocus');
        });

        ////////////////////////////////////////////////////////////////////////
        // Bind to the paste event of the input box so we know when we
        // get pasted data
        typer.bind('paste', function(e) {
            // wipe typer input clean just in case
            typer.val("");
            // this timeout is required because the onpaste event is
            // fired *before* the text is actually pasted
            setTimeout(function() {
                typer.consoleInsert(typer.val());
                typer.val("");
            }, 0);
        });
        //helper function to figure out new character added to input
        difference = function(value1, value2) {
            var output = [];
            for (i = 0; i < value2.length; i++) {
                if (value1[i] !== value2[i]) {
                    output.push(value2[i]);
                }
            }
            return output.join("");
        }
        ////////////////////////////////////////////////////////////////////////
        // Handle key hit before translation
        // For picking up control characters like up/left/down/right
        typer.keydown(function(e) {
            typer.oldValue = typer.val();
            oldValue = typer.val();
            cancelKeyPress = 0;
            var keyCode = e.which || e.code;
            // C-c: cancel the execution
            if (e.ctrlKey && keyCode == 67) {
                cancelKeyPress = keyCode;
                cancelExecution();
                return false;
            }
            if (acceptInput) {
                if (e.shiftKey && keyCode in shiftCodes) {
                    cancelKeyPress = keyCode;
                    (shiftCodes[keyCode])();
                    return false;
                } else if (e.altKey && keyCode in altCodes) {
                    cancelKeyPress = keyCode;
                    (altCodes[keyCode])();
                    return false;
                } else if (e.ctrlKey && keyCode in ctrlCodes) {
                    cancelKeyPress = keyCode;
                    (ctrlCodes[keyCode])();
                    return false;
                } else if (keyCode in keyCodes) {
                    cancelKeyPress = keyCode;
                    (keyCodes[keyCode])();
                    return false;
                }
            }
            if (isIgnorableKey(e)) {
                return false;
            }
            // C-v: don't insert on paste event
            if ((e.ctrlKey || e.metaKey) && String.fromCharCode(keyCode).toLowerCase() == 'v') {
                return true;
            }
            if (acceptInput && cancelKeyPress != keyCode && keyCode >= 32) {
                if (cancelKeyPress) return false;
            }


        });


        //after input event get new string, figure out the difference and add the character to the typer.
        typer.on("input", function(e) {
            var newValue = typer.val();
            typer.consoleInsert(difference(typer.oldValue, newValue));
            typer.val("");
        })


        function isIgnorableKey(e) {
            // for now just filter alt+tab that we receive on some platforms when
            // user switches windows (goes away from the browser)
            return ((e.keyCode == keyCodes.tab || e.keyCode == 192) && e.altKey);
        };

        ////////////////////////////////////////////////////////////////////////
        // Rotate through the command history
        function rotateHistory(n) {
            if (history.length == 0) return;
            ringn += n;
            if (ringn < 0) ringn = history.length;
            else if (ringn > history.length) ringn = 0;
            var prevText = promptText;
            if (ringn == 0) {
                promptText = restoreText;
            } else {
                promptText = history[ringn - 1];
            }
            if (config.historyPreserveColumn) {
                if (promptText.length < column + 1) {
                    column = promptText.length;
                } else if (column == 0) {
                    column = promptText.length;
                }
            } else {
                column = promptText.length;
            }
            updatePromptDisplay();
        };

        function previousHistory() {
            rotateHistory(-1);
        };

        function nextHistory() {
            rotateHistory(1);
        };

        // Add something to the history ring
        function addToHistory(line) {
            history.push(line);
            restoreText = '';
        };

        // Delete the character at the current position
        function deleteCharAtPos() {
            if (column < promptText.length) {
                promptText =
                    promptText.substring(0, column) +
                    promptText.substring(column + 1);
                restoreText = promptText;
                return true;
            } else return false;
        };

        function backDelete() {
            if (moveColumn(-1)) {
                deleteCharAtPos();
                updatePromptDisplay();
            }
        };

        function forwardDelete() {
            if (deleteCharAtPos()) {
                updatePromptDisplay();
            }
        };

        function deleteUntilEnd() {
            while (deleteCharAtPos()) {
                updatePromptDisplay();
            }
        };

        function clearCurrentPrompt() {
            extern.promptText("");
        };

        function clearScreen() {
            inner.children(".jquery-console-prompt-box, .jquery-console-message").slice(0, -1).remove();
            extern.report(" ");
            extern.focus();
        };

        function deleteNextWord() {
            // A word is defined within this context as a series of alphanumeric
            // characters.
            // Delete up to the next alphanumeric character
            while (
                column < promptText.length &&
                !isCharAlphanumeric(promptText[column])
            ) {
                deleteCharAtPos();
                updatePromptDisplay();
            }
            // Then, delete until the next non-alphanumeric character
            while (
                column < promptText.length &&
                isCharAlphanumeric(promptText[column])
            ) {
                deleteCharAtPos();
                updatePromptDisplay();
            }
        };

        function newLine() {
            var lines = promptText.split("\n");
            var last_line = lines.slice(-1)[0];
            var spaces = last_line.match(/^(\s*)/g)[0];
            var new_line = "\n" + spaces;
            promptText += new_line;
            moveColumn(new_line.length);
            updatePromptDisplay();
        };

        ////////////////////////////////////////////////////////////////////////
        // Validate command and trigger it if valid, or show a validation error
        function commandTrigger() {
            var line = promptText;
            if (typeof config.commandValidate == 'function') {
                var ret = config.commandValidate(line);
                if (ret == true || ret == false) {
                    if (ret) {
                        handleCommand();
                    }
                } else {
                    commandResult(ret, "jquery-console-message-error");
                }
            } else {
                handleCommand();
            }
        };

        // Scroll to the bottom of the view
        function scrollToBottom() {
            var version = jQuery.fn.jquery.split('.');
            var major = parseInt(version[0]);
            var minor = parseInt(version[1]);

            // check if we're using jquery > 1.6
            if ((major == 1 && minor > 6) || major > 1) {
                inner.prop({ scrollTop: inner.prop("scrollHeight") });
            } else {
                inner.attr({ scrollTop: inner.attr("scrollHeight") });
            }
        };

        function cancelExecution() {
            if (typeof config.cancelHandle == 'function') {
                config.cancelHandle();
            }
        }

        ////////////////////////////////////////////////////////////////////////
        // Handle a command
        function handleCommand() {
            if (typeof config.commandHandle == 'function') {
                disableInput();
                addToHistory(promptText);
                var text = promptText;
                if (extern.continuedPrompt) {
                    if (continuedText)
                        continuedText += '\n' + promptText;
                    else continuedText = promptText;
                } else continuedText = undefined;
                if (continuedText) text = continuedText;
                var ret = config.commandHandle(text, function(msgs) {
                    commandResult(msgs);
                });
                if (extern.continuedPrompt && !continuedText)
                    continuedText = promptText;
                if (typeof ret == 'boolean') {
                    if (ret) {
                        // Command succeeded without a result.
                        commandResult();
                    } else {
                        commandResult(
                            'Command failed.',
                            "jquery-console-message-error"
                        );
                    }
                } else if (typeof ret == "string") {
                    commandResult(ret, "jquery-console-message-success");
                } else if (typeof ret == 'object' && ret.length) {
                    commandResult(ret);
                } else if (extern.continuedPrompt) {
                    commandResult();
                }
            }
        };

        ////////////////////////////////////////////////////////////////////////
        // Disable input
        function disableInput() {
            acceptInput = false;
        };

        // Enable input
        function enableInput() {
            acceptInput = true;
        }

        ////////////////////////////////////////////////////////////////////////
        // Reset the prompt in invalid command
        function commandResult(msg, className) {
            column = -1;
            updatePromptDisplay();
            if (typeof msg == 'string') {
                message(msg, className);
            } else if ($.isArray(msg)) {
                for (var x in msg) {
                    var ret = msg[x];
                    message(ret.msg, ret.className);
                }
            } else { // Assume it's a DOM node or jQuery object.
                inner.append(msg);
            }
            newPromptBox();
        };

        ////////////////////////////////////////////////////////////////////////
        // Report some message into the console
        function report(msg, className) {
            var text = promptText;
            promptBox.remove();
            commandResult(msg, className);
            extern.promptText(text);
        };

        ////////////////////////////////////////////////////////////////////////
        // Display a message
        function message(msg, className) {
            var mesg = $('<div class="jquery-console-message"></div>');
            if (className) mesg.addClass(className);
            mesg.filledText(msg).hide();
            inner.append(mesg);
            mesg.show();
        };

        ////////////////////////////////////////////////////////////////////////
        // Handle normal character insertion
        // data can either be a number, which will be interpreted as the
        // numeric value of a single character, or a string
        typer.consoleInsert = function(data) {
            // TODO: remove redundant indirection
            var text = (typeof data == 'number') ? String.fromCharCode(data) : data;
            var before = promptText.substring(0, column);
            var after = promptText.substring(column);
            promptText = before + text + after;
            moveColumn(text.length);
            restoreText = promptText;
            updatePromptDisplay();
        };

        ////////////////////////////////////////////////////////////////////////
        // Move to another column relative to this one
        // Negative means go back, positive means go forward.
        function moveColumn(n) {
            if (column + n >= 0 && column + n <= promptText.length) {
                column += n;
                return true;
            } else return false;
        };

        function moveForward() {
            if (moveColumn(1)) {
                updatePromptDisplay();
                return true;
            }
            return false;
        };

        function moveBackward() {
            if (moveColumn(-1)) {
                updatePromptDisplay();
                return true;
            }
            return false;
        };

        function moveToStart() {
            if (moveColumn(-column))
                updatePromptDisplay();
        };

        function moveToEnd() {
            if (moveColumn(promptText.length - column))
                updatePromptDisplay();
        };

        function moveToNextWord() {
            while (
                column < promptText.length &&
                !isCharAlphanumeric(promptText[column]) &&
                moveForward()
            ) {}
            while (
                column < promptText.length &&
                isCharAlphanumeric(promptText[column]) &&
                moveForward()
            ) {}
        };

        function moveToPreviousWord() {
            // Move backward until we find the first alphanumeric
            while (
                column - 1 >= 0 &&
                !isCharAlphanumeric(promptText[column - 1]) &&
                moveBackward()
            ) {}
            // Move until we find the first non-alphanumeric
            while (
                column - 1 >= 0 &&
                isCharAlphanumeric(promptText[column - 1]) &&
                moveBackward()
            ) {}
        };

        function isCharAlphanumeric(charToTest) {
            if (typeof charToTest == 'string') {
                var code = charToTest.charCodeAt();
                return (code >= 'A'.charCodeAt() && code <= 'Z'.charCodeAt()) ||
                    (code >= 'a'.charCodeAt() && code <= 'z'.charCodeAt()) ||
                    (code >= '0'.charCodeAt() && code <= '9'.charCodeAt());
            }
            return false;
        };

        function doComplete() {
            if (typeof config.completeHandle == 'function') {
                doCompleteDirectly();
            } else {
                issueComplete();
            }
        };

        function doCompleteDirectly() {
            if (typeof config.completeHandle == 'function') {
                var completions = config.completeHandle(promptText);
                var len = completions.length;
                if (len === 1) {
                    extern.promptText(promptText + completions[0]);
                } else if (len > 1 && config.cols) {
                    var prompt = promptText;
                    // Compute the number of rows that will fit in the width
                    var max = 0;
                    for (var i = 0; i < len; i++) {
                        max = Math.max(max, completions[i].length);
                    }
                    max += 2;
                    var n = Math.floor(config.cols / max);
                    var buffer = "";
                    var col = 0;
                    for (i = 0; i < len; i++) {
                        var completion = completions[i];
                        buffer += completions[i];
                        for (var j = completion.length; j < max; j++) {
                            buffer += " ";
                        }
                        if (++col >= n) {
                            buffer += "\n";
                            col = 0;
                        }
                    }
                    commandResult(buffer, "jquery-console-message-value");
                    extern.promptText(prompt);
                }
            }
        };

        function issueComplete() {
            if (typeof config.completeIssuer == 'function') {
                config.completeIssuer(promptText);
            }
        };

        function showCompletion(promptText, completions) {

            var len = completions.length;
            if (len === 1) {
                extern.promptText(promptText + completions[0]);
            } else if (len > 1 && config.cols) {
                var prompt = promptText;
                // Compute the number of rows that will fit in the width
                var max = 0;
                for (var i = 0; i < len; i++) {
                    max = Math.max(max, completions[i].length);
                }
                max += 2;
                var n = Math.floor(config.cols / max);
                var buffer = "";
                var col = 0;
                for (i = 0; i < len; i++) {
                    var completion = completions[i];
                    buffer += completions[i];
                    for (var j = completion.length; j < max; j++) {
                        buffer += " ";
                    }
                    if (++col >= n) {
                        buffer += "\n";
                        col = 0;
                    }
                }
                commandResult(buffer, "jquery-console-message-value");
                extern.promptText(prompt);
            }
        };

        function doNothing() {};

        extern.promptText = function(text) {
            if (typeof text === 'string') {
                promptText = text;
                column = promptText.length;
                updatePromptDisplay();
            }
            return promptText;
        };

        ////////////////////////////////////////////////////////////////////////
        // Update the prompt display
        function updatePromptDisplay() {
            var line = promptText;
            var html = '';
            if (column > 0 && line == '') {
                // When we have an empty line just display a cursor.
                html = cursor;
            } else if (column == promptText.length) {
                // We're at the end of the line, so we need to display
                // the text *and* cursor.
                html = htmlEncode(line) + cursor;
            } else {
                // Grab the current character, if there is one, and
                // make it the current cursor.
                var before = line.substring(0, column);
                var current = line.substring(column, column + 1);
                if (current) {
                    current =
                        '<span class="jquery-console-cursor">' +
                        htmlEncode(current) +
                        '</span>';
                }
                var after = line.substring(column + 1);
                html = htmlEncode(before) + current + htmlEncode(after);
            }
            prompt.html(html);
            scrollToBottom();
        };

        // Simple HTML encoding
        // Simply replace '<', '>' and '&'
        // TODO: Use jQuery's .html() trick, or grab a proper, fast
        // HTML encoder.
        function htmlEncode(text) {
            return (
                text.replace(/&/g, '&amp;')
                .replace(/</g, '&lt;')
                .replace(/</g, '&lt;')
                .replace(/ /g, '&nbsp;')
                .replace(/\n/g, '<br />')
            );
        };

        return extern;
    };
    // Simple utility for printing messages
    $.fn.filledText = function(txt) {
        $(this).text(txt);
        $(this).html($(this).html().replace(/\t/g, '&nbsp;&nbsp;').replace(/\n/g, '<br/>'));
        return this;
    };

    // Alternative method for focus without scrolling
    $.fn.focusWithoutScrolling = function() {
        var x = window.scrollX,
            y = window.scrollY;
        $(this).focus();
        window.scrollTo(x, y);
    };
})(jQuery);