// SSE event dispatch (replaces the old tr-ext htmx extension + ws heartbeat).
//
// htmx 4's hx-sse extension handles the transport: it opens the single
// streaming connection (hx-sse:connect) and, for every *named* SSE event,
// re-dispatches it as a DOM event of that name carrying { data, id } in
// event.detail. *Unnamed* messages are auto-swapped into the DOM (our 'html'
// fragments, including OOB swaps), so they need no JS here.
//
// We only have to handle the 'rtc' event: parse its JSON and fan the typed
// messages out to the app handlers -- byte-for-byte the same behaviour as
// Ken's tr-ext transformResponse loop.

// Don't pause signalling when a tab is backgrounded; a WebSocket wouldn't,
// and pausing would look like a disconnect to peers. (Auto-reconnect stays on.)
htmx.config.sse = { pauseOnBackground: false };

function handle_rtc_event(event) {
    const data = JSON.parse(event.detail.data);
    for (var [app, message] of Object.entries(data)) {
        apps._forward(app, message.type, message);
    }
}

// The 'rtc' CustomEvent bubbles up from the hx-sse:connect element to document.
document.addEventListener('rtc', handle_rtc_event);

const apps = {
    '_add': function(app, name, target) {
        if (!apps[app]) {
            apps[app] = new Map();
        }
        if (!apps[app].has(name)) {
            apps[app].set(name, new Array());
        }
        if (!apps[app].get(name).includes(target)) {
            apps[app].get(name).push(target);
        }
    },
    '_remove': function(app, name, target) {
        if (apps[app]) {
            idx = apps[app].get(name).indexOf(target)
            if (idx > -1) {
                apps[app].get(name).splice(idx, 1);
            }
            if (apps[app].get(name).length == 0) {
                apps[app].delete(name);
            }
        }
    },
    '_forward': function(app, name, message) {
        if (apps[app] && apps[app].has(name)) {
            for (target of apps[app].get(name)) {
                target(message);
            }
        }
    },
}
