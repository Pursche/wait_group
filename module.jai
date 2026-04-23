Wait_Group :: struct {
    verbose: bool;
    #if OS == .MACOS {
        kevent_queue: s32 = -1;
    } else #if OS == .LINUX {
        epoll_handle: s32 = -1;
        signals_by_handle: Table(s32, s32); // Map from handle to signal
    } else #if OS == .WINDOWS {
        // Self-pipe: a pair of loopback UDP sockets used to wake wait_for_events
        // from another thread (trigger_user_event) and from the console-ctrl
        // handler. Sending a 1-byte datagram on wakeup_send makes wakeup_recv
        // readable; the wait loop drains it and dispatches whatever's queued
        // in pending_triggers / pending_signals.
        wakeup_recv:      s32 = -1;
        wakeup_send:      s32 = -1;
        wakeup_mutex:     Mutex;
        wakeup_initialized: bool;
        pending_triggers: [..] User_Event_Id;
        pending_signals:  [..] s32;

        // Sorted-by-deadline list of active timers. Tiny N in practice (MCP
        // usage: zero timers 99% of the time), so a linear scan per fire is
        // fine. Keep sorted so wait_for_events can read [0].deadline_ms for
        // the WSAPoll timeout without a full pass.
        timers: [..] Timer_Entry;
    } else {
        #assert(false);
    }

    fd_handlers:     Table(Fd_Data, Fd_Handler, given_compare_function = fd_data_is_equal, given_hash_function = hash_fd_data);
    signal_handlers: Table(s32, Signal_Handler);
    user_handlers:   Table(User_Event_Id, User_Handler);

    Fd_Data :: struct {
        fd:     s32;
        event: Wait_Event_Type;
    }

    fd_data_is_equal :: (a: Fd_Data, b: Fd_Data) -> bool {
        return a.fd == b.fd && a.event == b.event;
    }
    hash_fd_data :: (data: Fd_Data) -> u32 {
        return Hash.get_hash(data.fd) ^ Hash.get_hash(cast(int) data.event);
    }
}

Wait_Event_Type :: enum {
    READ;
    WRITE;
}

User_Event_Id :: #type,isa u64;

Fd_Handler :: struct {
    data: *void;
    callback: #type (group: *Wait_Group, handle: s32, is_eof: bool, event: Wait_Event_Type, data: *void);
}

Signal_Handler :: struct {
    data: *void;
    callback: #type (group: *Wait_Group, signal: s32, data: *void);
}

User_Handler :: struct {
    data: *void;
    callback: #type (group: *Wait_Group, event_id: User_Event_Id, data: *void);
}


init :: (group: *Wait_Group, verbose := false) -> success: bool {
    group.verbose = verbose;
    remember_allocators(*group.fd_handlers);
    remember_allocators(*group.signal_handlers);
    remember_allocators(*group.user_handlers);

    #if OS == .LINUX {
        group.epoll_handle = epoll_create1(EPOLL_CLOEXEC);
        if group.epoll_handle < 0 {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not create epoll instance: % %", error_code, error_string);
            return false;
        }

        remember_allocators(*group.signals_by_handle);
    } else #if OS == .MACOS {
        // @ToDo: This initialization itself is not thread safe
        // We could do a fancy compare_and_swap loop here like in ensure_synchronization_is_initted
        // But for now we just ensure that our first wait group is initialized before we do threaded stuff
        if next_user_event_id == 0 {
            init(*next_user_event_id_mutex);
            next_user_event_id = 1;
        }

        group.kevent_queue = kqueue();
        if group.kevent_queue == -1 {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not create event queue: % %", error_code, error_string);
            return false;
        }
    } else #if OS == .WINDOWS {
        // Winsock must be initialized before any socket calls. Hyperserve
        // callers typically already did this; socket_init is idempotent.
        if !socket_init()  return false;

        // Same app-level id counter as macOS (kernel doesn't assign ids).
        if next_user_event_id == 0 {
            init(*next_user_event_id_mutex);
            next_user_event_id = 1;
        }

        init(*group.wakeup_mutex);

        // Self-pipe: two UDP sockets on the loopback — recv listens, send is
        // connected to recv. trigger_user_event sends a 1-byte packet on send
        // to wake the wait loop.
        recv_sock := socket(AF_INET, .DGRAM, 0);
        if recv_sock == INVALID_SOCKET {
            log_error("wait_group(windows): socket(recv) failed");
            return false;
        }
        if bind(recv_sock, "127.0.0.1", 0) == -1 {
            log_error("wait_group(windows): bind(recv) failed");
            closesocket(recv_sock);
            return false;
        }
        recv_addr: sockaddr_in;
        recv_addr_len: s32 = size_of(sockaddr_in);
        if getsockname(recv_sock, cast(*sockaddr)*recv_addr, *recv_addr_len) == -1 {
            log_error("wait_group(windows): getsockname(recv) failed");
            closesocket(recv_sock);
            return false;
        }
        if !set_blocking(recv_sock, false) {
            closesocket(recv_sock);
            return false;
        }

        send_sock := socket(AF_INET, .DGRAM, 0);
        if send_sock == INVALID_SOCKET {
            log_error("wait_group(windows): socket(send) failed");
            closesocket(recv_sock);
            return false;
        }
        // Connect so future sends() go to the recv side without specifying
        // a destination each time.
        if connect(send_sock, cast(*sockaddr)*recv_addr, size_of(sockaddr_in)) == -1 {
            log_error("wait_group(windows): connect(send) failed");
            closesocket(recv_sock);
            closesocket(send_sock);
            return false;
        }

        group.wakeup_recv        = cast(s32) recv_sock;
        group.wakeup_send        = cast(s32) send_sock;
        group.wakeup_initialized = true;
    }
    return true;
}

deinit :: (group: *Wait_Group) {
    #if OS == .LINUX {
        close_and_reset_fd(*group.epoll_handle);
        for signal, handle: group.signals_by_handle {
            close(handle);
        }
        deinit(*group.signals_by_handle);
        group.signals_by_handle.entries.data = null;
    } else #if OS == .MACOS {
        close_and_reset_fd(*group.kevent_queue);
    } else #if OS == .WINDOWS {
        if gSignalRecipient == group  _RestoreConsoleCtrlHandler();

        if group.wakeup_recv >= 0  closesocket(cast(Socket) group.wakeup_recv);
        if group.wakeup_send >= 0  closesocket(cast(Socket) group.wakeup_send);
        group.wakeup_recv = -1;
        group.wakeup_send = -1;
        if group.wakeup_initialized {
            destroy(*group.wakeup_mutex);
            group.wakeup_initialized = false;
        }
        array_reset(*group.pending_triggers);
        array_reset(*group.pending_signals);
        array_reset(*group.timers);
    } else {
        #assert(false);
    }

    deinit(*group.fd_handlers);
    group.fd_handlers.entries.data = null;
    deinit(*group.signal_handlers);
    group.signal_handlers.entries.data = null;
    deinit(*group.user_handlers);
    group.user_handlers.entries.data = null;
}

add_handles :: (group: *Wait_Group, data: *$T, callback: #type (group: *Wait_Group, handle: s32, is_eof: bool, event: Wait_Event_Type, data: *T), handles: .. s32, event_type := Wait_Event_Type.READ) -> success: bool {
    if group.verbose log("Adding handles % for event type %", handles, event_type);
    #if OS == .LINUX {
        for handles {
            action := Epoll_Ctl.ADD;
            events: u32;
            if #complete event_type == {
                case .READ;  events = EPOLLRDHUP | EPOLLIN;
                case .WRITE; events = EPOLLOUT;
            }
            for fd_handler, fd_data: group.fd_handlers {
                if fd_data.fd == it {
                    assert(fd_data.event != event_type, "Trying to add handle % with event %, but there is already a handler for that!", it, event_type);
                    action = .MOD;
                    if #complete fd_data.event == {
                        case .READ;  events |= EPOLLRDHUP | EPOLLIN;
                        case .WRITE; events |= EPOLLOUT;
                    }
                }
            }

            success := update_epoll_handle(group, it, action, events);
            if !success return false;
        }
    } else #if OS == .MACOS {
        events := NewArray(handles.count, Kevent64,, temp);
        for handles {
            events[it_index].ident = cast(u64) it;
            events[it_index].flags = .ADD;
            if #complete event_type == {
                case .READ;  events[it_index].filter = .READ;
                case .WRITE; events[it_index].filter = .WRITE;
            }
        }

        result := kevent64(group.kevent_queue, events.data, cast (s32) events.count, null, 0, 0, null);
        if result == -1 {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not register events with the event queue: % %", error_code, error_string);
            return false;
        }
    } else #if OS == .WINDOWS {
        // WSAPoll takes the fd array on every wait_for_events call — no kernel
        // registration, so just record the handler in the table. The wait
        // loop rebuilds the pollfd array from fd_handlers each iteration.
    }

    handler: Fd_Handler;
    handler.data = data;
    handler.callback = xx callback;
    for handles {
        fd_data: Wait_Group.Fd_Data;
        fd_data.fd = it;
        fd_data.event = event_type;
        assert(!table_find_pointer(*group.fd_handlers, fd_data), "Trying to add handle % with event %, but there is already a handler for that!", it, event_type);
        table_add(*group.fd_handlers, fd_data, handler);
    }
    return true;
}

add_signals :: (group: *Wait_Group, data: *$T, callback: #type (group: *Wait_Group, signal: s32, data: *T), signals: ..s32) -> success: bool {
    if group.verbose log("Adding signals %", signals);
    #if OS == .LINUX {
        for signals {
            handle, success := create_signal_handle(it);
            if !success {
                // @ToDo: Close and remove handles already created
                return false;
            }

            if group.verbose log("Created signal handle % for signal %", handle, signal);

            table_add(*group.signals_by_handle, handle, it);

            success = update_epoll_handle(group, handle, .ADD, EPOLLIN, description = "signal handle");
            if !success return false;
        }
    } else #if OS == .MACOS {
        events := NewArray(signals.count, Kevent64,, temp);
        for signals {
            events[it_index].ident = cast(u64) it;
            events[it_index].filter = .SIGNAL;
            events[it_index].flags = .ADD;
        }
        result := kevent64(group.kevent_queue, events.data, cast (s32) events.count, null, 0, 0, null);
        if result == -1 {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not register signal events with the event queue: % %", error_code, error_string);
            return false;
        }
    } else #if OS == .WINDOWS {
        // Windows has no POSIX signals. We map onto SetConsoleCtrlHandler —
        // the process-global handler pushes the equivalent signal number into
        // group.pending_signals and triggers the wakeup pipe. The wait loop
        // then dispatches to signal_handlers. Only one wait_group at a time
        // can receive signals; later callers replace earlier ones.
        if !_InstallConsoleCtrlHandler(group) {
            log_error("wait_group(windows): could not install console-ctrl handler");
            return false;
        }
    }

    handler: Signal_Handler;
    handler.data = data;
    handler.callback = xx callback;
    for signals {
        assert(!table_find_pointer(*group.signal_handlers, it), "Trying to add signal %, but there is already a handler for that!", it);
        table_add(*group.signal_handlers, it, handler);
    }

    return true;
}

add_user_event :: (group: *Wait_Group, data: *$T, callback: #type (group: *Wait_Group, event_id: User_Event_Id, data: *T)) -> success: bool, event_id: User_Event_Id {
    event_id: User_Event_Id;

    #if OS == .LINUX {
        handle := eventfd(0, xx EFD.NONBLOCK);
        if handle == -1 {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not create user event handle: % %", error_code, error_string);
            return false, 0;
        }

        success := update_epoll_handle(group, handle, .ADD, EPOLLIN, description = "user event handle");
        if !success return false, 0;

        event_id = cast(User_Event_Id) handle;
    } else #if OS == .MACOS {
        {
            lock(*next_user_event_id_mutex);
            defer unlock(*next_user_event_id_mutex);
            event_id = next_user_event_id;
            next_user_event_id += 1;
        }

        events: [1] Kevent64;
        events[0].ident = event_id;
        events[0].filter = .USER;
        events[0].flags = .ADD;
        events[0].fflags = .FFNOP;
        result := kevent64(group.kevent_queue, events.data, cast (s32) events.count, null, 0, 0, null);
        if result == -1 {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not register user event % with the event queue: % %", event_id, error_code, error_string);
            return false, 0;
        }
    } else #if OS == .WINDOWS {
        // Assign a fresh app-level id; the wait loop uses user_handlers
        // + pending_triggers to dispatch when a trigger_user_event fires.
        lock(*next_user_event_id_mutex);
        defer unlock(*next_user_event_id_mutex);
        event_id = next_user_event_id;
        next_user_event_id += 1;
    }

    if group.verbose log("Adding user event %", event_id);
    handler: User_Handler;
    handler.data = data;
    handler.callback = xx callback;
    table_add(*group.user_handlers, event_id, handler);

    return true, event_id;
}

add_timer :: (group: *Wait_Group, data: *$T, callback: #type (group: *Wait_Group, handle: User_Event_Id, data: *T), timeout_ms: s64, recurring: bool) -> success: bool, User_Event_Id {
    event_id: User_Event_Id;
    #if OS == .LINUX {
        handle := timerfd_create(xx clockid_t.MONOTONIC, xx TFD.NONBLOCK);
        if handle == -1 {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not create timer event handle: % %", error_code, error_string);
            return false, 0;
        }

        timeout_spec: timespec;
        timeout_spec.tv_sec = timeout_ms / 1000;
        timeout_spec.tv_nsec = (timeout_ms % 1000) * 1000 * 1000;

        timer_spec: itimerspec;
        timer_spec.it_value = timeout_spec;
        if recurring {
            timer_spec.it_interval = timeout_spec;
        }

        result := timerfd_settime(handle, 0, *timer_spec, null);
        if (result == -1) {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not set timer for timer event handle: % %", error_code, error_string);
            close(handle);
            return false, 0;
        }

        success := update_epoll_handle(group, handle, .ADD, EPOLLIN, description = "timer event handle");
        if !success return false, 0;

        event_id = cast(User_Event_Id) handle;
    } else #if OS == .MACOS {
        {
            lock(*next_user_event_id_mutex);
            defer unlock(*next_user_event_id_mutex);
            event_id = next_user_event_id;
            next_user_event_id += 1;
        }

        flags := Kevent_Flags.ADD;
        if !recurring flags |= .ONESHOT;

        events: [1] Kevent64;
        events[0].ident = event_id;
        events[0].filter = .TIMER;
        events[0].flags = flags;
        events[0].data = timeout_ms;

        result := kevent64(group.kevent_queue, events.data, cast (s32) events.count, null, 0, 0, null);
        if result == -1 {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not register user event % with the event queue: % %", event_id, error_code, error_string);
            return false, 0;
        }
    } else #if OS == .WINDOWS {
        lock(*next_user_event_id_mutex);
        {
            defer unlock(*next_user_event_id_mutex);
            event_id = next_user_event_id;
            next_user_event_id += 1;
        }

        entry: Timer_Entry;
        entry.id          = event_id;
        entry.deadline_ms = _NowMs() + timeout_ms;
        entry.interval_ms = timeout_ms;
        entry.recurring   = recurring;

        lock(*group.wakeup_mutex);
        defer unlock(*group.wakeup_mutex);
        _InsertTimerSorted(*group.timers, entry);
    } else {
        #assert(false); // Incomplete
    }

    if group.verbose log("Adding %ms timer (recurring: %) as user event %", timeout_ms, recurring, event_id);
    handler: User_Handler;
    handler.data = data;
    handler.callback = xx callback;
    table_add(*group.user_handlers, event_id, handler);

    return true, event_id;
}

reset_timer :: (group: *Wait_Group, timer: User_Event_Id, timeout_ms: s64, recurring: bool) -> success: bool {
    if group.verbose log("Resetting timer % for %ms (recurring: %)", timer, timeout_ms, recurring);
    #if OS == .LINUX {
        timeout_spec: timespec;
        timeout_spec.tv_sec = timeout_ms / 1000;
        timeout_spec.tv_nsec = (timeout_ms % 1000) * 1000 * 1000;

        timer_spec: itimerspec;
        timer_spec.it_value = timeout_spec;
        if recurring {
            timer_spec.it_interval = timeout_spec;
        }

        result := timerfd_settime(cast(s32) timer, 0, *timer_spec, null);
        if (result == -1) {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not set timer for timer event handle: % %", error_code, error_string);
            return false;
        }
    } else #if OS == .MACOS {
        flags := Kevent_Flags.ADD;
        if !recurring flags |= .ONESHOT;

        events: [1] Kevent64;
        events[0].ident = timer;
        events[0].filter = .TIMER;
        events[0].flags = flags;
        events[0].data = timeout_ms;

        result := kevent64(group.kevent_queue, events.data, cast (s32) events.count, null, 0, 0, null);
        if result == -1 {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not register user event % with the event queue: % %", timer, error_code, error_string);
            return false;
        }
    } else #if OS == .WINDOWS {
        lock(*group.wakeup_mutex);
        defer unlock(*group.wakeup_mutex);

        for i: 0..group.timers.count - 1 {
            if group.timers[i].id == timer {
                group.timers[i].deadline_ms = _NowMs() + timeout_ms;
                group.timers[i].interval_ms = timeout_ms;
                group.timers[i].recurring   = recurring;
                entry := group.timers[i];
                array_unordered_remove_by_index(*group.timers, i);
                _InsertTimerSorted(*group.timers, entry);
                return true;
            }
        }
        log_error("wait_group(windows): reset_timer called with unknown id %", timer);
        return false;
    } else {
        #assert(false); // Incomplete
    }
    return true;
}

trigger_user_event :: (group: *Wait_Group, event_id: User_Event_Id) -> success: bool {
    if group.verbose log("Triggering user event %", event_id);
    assert(table_find_pointer(*group.user_handlers, event_id) != null, "event_id % is not a user event", event_id);
    #if OS == .LINUX {
        value: u64 = 1;
        size := size_of(type_of(value));
        result := write(cast(s32) event_id, *value, cast(u64) size);
        if result != size {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not write to user event %: % %", event_id, error_code, error_string);
            return false;
        }
    } else #if OS == .MACOS {
        events: [1] Kevent64;
        events[0].ident = event_id;
        events[0].filter = .USER;
        events[0].flags = .ENABLE;
        events[0].fflags = Kevent_Fflags.FFCOPY | .TRIGGER | 0x01;
        result := kevent64(group.kevent_queue, events.data, cast (s32) events.count, null, 0, 0, null);
        if result == -1 {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not trigger user event % with the event queue: % %", event_id, error_code, error_string);
            return false;
        }
    } else #if OS == .WINDOWS {
        // Queue the id + poke the self-pipe so the wait loop wakes up and
        // dispatches. This is the cross-thread path — callers are not always
        // on the wait loop's thread.
        lock(*group.wakeup_mutex);
        array_add(*group.pending_triggers, event_id);
        unlock(*group.wakeup_mutex);

        b: u8 = 1;
        sent := send(cast(Socket) group.wakeup_send, *b, 1, xx 0);
        if sent != 1 {
            error_code, error_string := System.get_error_value_and_string();
            log_error("wait_group(windows): could not write wakeup byte: % %", error_code, error_string);
            return false;
        }
    } else {
        #assert(false);
    }

    return true;
}

remove_handle :: (group: *Wait_Group, handle: s32, event_type := Wait_Event_Type.READ) -> success: bool {
    if group.verbose log("Removing handle % for event type %", handle, event_type);

    fd_data: Wait_Group.Fd_Data;
    fd_data.fd = handle;
    fd_data.event = event_type;
    table_remove(*group.fd_handlers, fd_data);

    #if OS == .LINUX {
        action := Epoll_Ctl.DEL;
        events: u32;
        for fd_handler, fd_data: group.fd_handlers {
            if fd_data.fd == handle {
                assert(fd_data.event != event_type, "Trying to remove handle % with event %, but there is more than one handler for that!", handle, event_type);
                action = .MOD;
                if #complete fd_data.event == {
                    case .READ;  events |= EPOLLRDHUP | EPOLLIN;
                    case .WRITE; events |= EPOLLOUT;
                }
            }
        }

        success := update_epoll_handle(group, handle, action, events);
        if !success return false;
    } else #if OS == .MACOS {
        events: [1] Kevent64;
        events[0].ident = cast(u64) handle;
        if #complete event_type == {
            case .READ;  events[0].filter = .READ;
            case .WRITE; events[0].filter = .WRITE;
        }
        events[0].flags = .DELETE;

        result := kevent64(group.kevent_queue, events.data, cast (s32) events.count, null, 0, 0, null);
        if result == -1 {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not remove handle % from the event queue: % %", handle, error_code, error_string);
            return false;
        }
    } else #if OS == .WINDOWS {
        // Table removal above is all that's needed — WSAPoll fd list is
        // rebuilt from fd_handlers each iteration.
    } else {
        #assert(false);
    }

    return true;
}

// @ToDo: remove_signal

remove_user_event :: (group: *Wait_Group, event_id: User_Event_Id) -> success: bool {
    return remove_user_event_or_timer(group, event_id, is_timer = false);
}

remove_timer :: (group: *Wait_Group, timer_id: User_Event_Id) -> success: bool {
    return remove_user_event_or_timer(group, timer_id, is_timer = true);
}

#if OS == .LINUX {
    update_epoll_handle :: (group: *Wait_Group, handle: s32, action: Epoll_Ctl, events: u32, description := "handle") -> success: bool {
        eev: epoll_event;
        eev.data.fd = handle;
        eev.events = events;

        result := epoll_ctl(group.epoll_handle, action, handle, *eev);
        if result == -1 {
            error_code := System.get_error_value();
            if error_code == EBADF && action == .DEL {
                // handle was probably already closed and therefore auto-removed from epoll.
                return true;
            }

            error_string := System.get_error_string(error_code);
            log_error("Could not perform epoll % on % %: % %", action, description, handle, error_code, error_string);
            return false;
        }

        return true;
    }
}

wait_for_events :: (group: *Wait_Group, timeout_ms := -1) -> success: bool {
    success := true;

    #if OS == .LINUX {
        clear :: (handle: s32, $T: Type, description: string) -> bool {
            while true {
                info: T;
                bytes_read := repeat_if_interrupted(read(handle, *info, size_of(type_of(info))));
                if bytes_read == -1 {
                    error_value := errno();
                    if error_value != EAGAIN && error_value != EWOULDBLOCK {
                        error_string := System.get_error_string(error_value);
                        log_error("Could not read % from handle %: % %", description, handle, error_value, error_string);
                        return false;
                    }
                    break;
                }
            }
            return true;
        }

        poll_timeout_ms := cast(s32) timeout_ms;
        events: [16] epoll_event;

        sigset: sigset_t;
        sigemptyset(*sigset);
        sigaddset(*sigset, SIGCHLD);  // default: ignore
        sigaddset(*sigset, SIGCONT);  // default: continue execution
        sigaddset(*sigset, SIGURG);   // default: ignore
        sigaddset(*sigset, SIGWINCH); // default: ignore

        num_events := repeat_if_interrupted(epoll_wait(group.epoll_handle, events.data, events.count, poll_timeout_ms));
        if num_events == -1 {
            error_value, error_string := System.get_error_value_and_string();
            log_error("Couldn't poll events: % %", error_value, error_string);
            return false;
        }

        if group.verbose log("Wait: % events", num_events);
        for i: 0..num_events-1 {
            it := events[i];
            handle := it.data.fd;
            if group.verbose log("Event: %, %", handle, it.events);

            if it.events & EPOLLNVAL {
                log_error("Handle % reported invalid value: %", handle, it.events);
                success = false;
            }

            is_eof := (it.events & EPOLLHUP) != 0;
            if it.events & EPOLLIN || is_eof {
                signal := table_find_pointer(*group.signals_by_handle, handle);
                if signal {
                    // Clear the signal event by reading the signal info
                    if !clear(handle, signalfd_siginfo, "signal info") {
                        success = false;
                    }

                    handler := table_find_pointer(*group.signal_handlers, signal.*);
                    if handler handler.callback(group, signal.*, handler.data);
                    continue;
                }

                {
                    event_id := cast(User_Event_Id) handle;
                    handler := table_find_pointer(*group.user_handlers, event_id);
                    if handler {
                        // Clear the user/timer event by reading the counters
                        if !clear(handle, u64, "event/timer") {
                            success = false;
                        }

                        handler.callback(group, event_id, handler.data);
                        continue;
                    }
                }

                {
                    fd_data: Wait_Group.Fd_Data;
                    fd_data.fd = handle;
                    fd_data.event = .READ;
                    handler := table_find_pointer(*group.fd_handlers, fd_data);
                    if handler handler.callback(group, handle, is_eof, .READ, handler.data);
                    // Don’t stop here, in case POLLOUT is also set!
                }
            }

            if it.events & EPOLLOUT || is_eof {
                fd_data: Wait_Group.Fd_Data;
                fd_data.fd = handle;
                fd_data.event = .WRITE;
                handler := table_find_pointer(*group.fd_handlers, fd_data);
                if handler handler.callback(group, handle, is_eof, .WRITE, handler.data);
            }
        }
    } else #if OS == .MACOS {
        timeout_spec_pointer: *timespec = null;
        timeout_spec: timespec;
        if timeout_ms >= 0 {
            timeout_spec.tv_sec = timeout_ms / 1000;
            timeout_spec.tv_nsec = (timeout_ms % 1000) * 1000 * 1000;
            timeout_spec_pointer = *timeout_spec;
        }

        // @ToDo: Max events?
        events: [64] Kevent64;
        num_events := kevent64(group.kevent_queue, null, 0, events.data, events.count, 0, timeout_spec_pointer);
        if num_events == -1 {
            error_code := System.get_error_value();
            if error_code == EINTR {
                // Got interrupted, try again.
                return true;
            }

            error_string := System.get_error_string(error_code);
            log_error("Could not read events from the event queue: % %\n", error_code, error_string);
            return false;
        }

        for 0..num_events-1 {
            event := events[it];
            if event.filter == .SIGNAL {
                signal := cast(s32) event.ident;
                handler := table_find_pointer(*group.signal_handlers, signal);
                if handler handler.callback(group, signal, handler.data);
            } else if event.filter == .READ {
                handle := cast(s32) event.ident;
                fd_data: Wait_Group.Fd_Data;
                fd_data.fd = handle;
                fd_data.event = .READ;
                handler := table_find_pointer(*group.fd_handlers, fd_data);
                if handler {
                    is_eof := (event.flags & .EOF) != 0;
                    handler.callback(group, handle, is_eof, .READ, handler.data);
                }
            } else if event.filter == .WRITE {
                handle := cast(s32) event.ident;
                fd_data: Wait_Group.Fd_Data;
                fd_data.fd = handle;
                fd_data.event = .WRITE;
                handler := table_find_pointer(*group.fd_handlers, fd_data);
                if handler {
                    is_eof := (event.flags & .EOF) != 0;
                    handler.callback(group, handle, is_eof, .WRITE, handler.data);
                }
            } else if event.filter == .USER || event.filter == .TIMER {
                event_id := cast(User_Event_Id) event.ident;
                handler := table_find_pointer(*group.user_handlers, event_id);
                if handler handler.callback(group, event_id, handler.data);
            }
        }
    } else #if OS == .WINDOWS {
        // Build a WSAPOLLFD array on each call from fd_handlers + the
        // wakeup-recv socket. WSAPoll is level-triggered like epoll here —
        // no per-fd registration state to maintain.
        //
        // Timeout is min(caller_timeout, next_timer_deadline). If a timer
        // fires before any I/O event, WSAPoll returns 0 and we dispatch
        // the expired timer(s) ourselves.

        fds: [..] pollfd;
        fds.allocator = temp;

        // Wakeup pipe is always first — handy when scanning for it.
        wpfd: pollfd;
        wpfd.fd     = cast(SOCKET) group.wakeup_recv;
        wpfd.events = cast(s16) POLLIN;
        array_add(*fds, wpfd);

        for fd_handler, fd_data: group.fd_handlers {
            pfd: pollfd;
            pfd.fd = cast(SOCKET) fd_data.fd;
            if #complete fd_data.event == {
                case .READ;  pfd.events = cast(s16) POLLIN;
                case .WRITE; pfd.events = cast(s16) POLLOUT;
            }
            array_add(*fds, pfd);
        }

        // Compute poll timeout accounting for the nearest timer.
        poll_ms: s32 = cast(s32) timeout_ms;
        now := _NowMs();
        {
            lock(*group.wakeup_mutex);
            defer unlock(*group.wakeup_mutex);
            if group.timers.count > 0 {
                deadline := group.timers[0].deadline_ms;
                until := deadline - now;
                if until < 0  until = 0;
                if poll_ms < 0 || until < poll_ms  poll_ms = cast(s32) until;
            }
        }

        num_events := WSAPoll(fds.data, cast(u32) fds.count, poll_ms);
        if num_events == -1 {
            error_code, error_string := System.get_error_value_and_string();
            log_error("wait_group(windows): WSAPoll failed: % %", error_code, error_string);
            return false;
        }

        // 1) Expired timers (even when num_events == 0 — timeout hit).
        {
            fired: [..] User_Event_Id;
            fired.allocator = temp;
            recurring_to_reinsert: [..] Timer_Entry;
            recurring_to_reinsert.allocator = temp;

            now = _NowMs();
            lock(*group.wakeup_mutex);
            while group.timers.count > 0 && group.timers[0].deadline_ms <= now {
                entry := group.timers[0];
                array_unordered_remove_by_index(*group.timers, 0);
                // Re-sort since we just disturbed [0]. Small N, simple.
                _SortTimers(*group.timers);
                array_add(*fired, entry.id);
                if entry.recurring {
                    next := entry;
                    next.deadline_ms = now + entry.interval_ms;
                    array_add(*recurring_to_reinsert, next);
                }
            }
            for recurring_to_reinsert  _InsertTimerSorted(*group.timers, it);
            unlock(*group.wakeup_mutex);

            for fired {
                handler := table_find_pointer(*group.user_handlers, it);
                if handler handler.callback(group, it, handler.data);
            }
        }

        if num_events == 0  return success;

        // 2) Wakeup pipe — drain the pending triggers + signals queues.
        if (fds[0].revents & cast(s16) POLLIN) != 0 {
            drain_buf: [256] u8;
            while true {
                got := recv(cast(Socket) group.wakeup_recv, drain_buf.data, cast(s32) drain_buf.count, xx 0);
                if got <= 0  break;
            }

            fired_triggers: [..] User_Event_Id;
            fired_triggers.allocator = temp;
            fired_signals: [..] s32;
            fired_signals.allocator  = temp;

            lock(*group.wakeup_mutex);
            for group.pending_triggers  array_add(*fired_triggers, it);
            array_reset(*group.pending_triggers);
            for group.pending_signals   array_add(*fired_signals, it);
            array_reset(*group.pending_signals);
            unlock(*group.wakeup_mutex);

            for fired_triggers {
                handler := table_find_pointer(*group.user_handlers, it);
                if handler handler.callback(group, it, handler.data);
            }
            for fired_signals {
                handler := table_find_pointer(*group.signal_handlers, it);
                if handler handler.callback(group, it, handler.data);
            }
        }

        // 3) Socket FD events.
        for i: 1..fds.count - 1 {
            pfd := fds[i];
            if pfd.revents == 0  continue;
            handle := cast(s32) pfd.fd;

            is_eof := (pfd.revents & cast(s16) (POLLHUP | POLLERR)) != 0;

            if (pfd.revents & cast(s16) POLLIN) != 0 || is_eof {
                fd_data: Wait_Group.Fd_Data;
                fd_data.fd = handle;
                fd_data.event = .READ;
                handler := table_find_pointer(*group.fd_handlers, fd_data);
                if handler handler.callback(group, handle, is_eof, .READ, handler.data);
            }
            if (pfd.revents & cast(s16) POLLOUT) != 0 || is_eof {
                fd_data: Wait_Group.Fd_Data;
                fd_data.fd = handle;
                fd_data.event = .WRITE;
                handler := table_find_pointer(*group.fd_handlers, fd_data);
                if handler handler.callback(group, handle, is_eof, .WRITE, handler.data);
            }
        }
    } else {
        #assert(false);
    }

    return success;
}

#scope_module

close_and_reset_fd :: (fd: *s32, loc := #caller_location) {
    if fd.* != -1 {
        result := close(fd.*);
        if result == -1 {
            error_code, error_string := System.get_error_value_and_string();
            log_error("%: %,%: Could not close file descriptor %: % %", loc.fully_pathed_filename, loc.line_number, loc.character_number, fd, error_code, error_string);
        }
        fd.* = -1;
    }
}

#scope_file

repeat_if_interrupted :: (call: Code) -> s64 #expand {
    while true {
        result := #insert call;
        if result < 0 {
            error_code := errno();
            if error_code == EINTR continue; // Got interrupted, try again
        }

        return result;
    }
}


remove_user_event_or_timer :: (group: *Wait_Group, event_id: User_Event_Id, $is_timer: bool) -> success: bool {
    #if is_timer {
        NAME :: "timer";
    } else {
        NAME :: "user event";
    }
    if group.verbose log("Removing % %", NAME, event_id);

    found := table_remove(*group.user_handlers, event_id);
    assert(found, "Could not find % %", NAME, event_id);

    #if OS == .LINUX {
        handle := cast(s32) event_id;
        result := close(handle);
        if result == -1 {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not close % %: % %", NAME, event_id, error_code, error_string);
            return false;
        }
    } else #if OS == .MACOS {
        events: [1] Kevent64;
        events[0].ident = event_id;
        events[0].flags = .DELETE;
        #if is_timer {
            events[0].filter = .TIMER;
        } else {
            events[0].filter = .USER;
        }

        result := kevent64(group.kevent_queue, events.data, cast (s32) events.count, null, 0, 0, null);
        if result == -1 {
            error_code := System.get_error_value();
            #if is_timer {
                if error_code == ENOENT {
                    // ONESHOT timers are automatically removed, so if the timer has already fired,
                    // we will get this error if we then try to remove it.
                    return true;
                }
            }
            error_string := System.get_error_string(error_code);
            log_error("Could not remove % % from the event queue: % %", NAME, event_id, error_code, error_string);
            return false;
        }
    } else #if OS == .WINDOWS {
        #if is_timer {
            lock(*group.wakeup_mutex);
            defer unlock(*group.wakeup_mutex);
            for i: 0..group.timers.count - 1 {
                if group.timers[i].id == event_id {
                    array_unordered_remove_by_index(*group.timers, i);
                    _SortTimers(*group.timers);
                    return true;
                }
            }
            // Already expired ONESHOT — consistent with macOS behavior.
            return true;
        } else {
            // User event: nothing beyond the table removal already done above.
        }
    } else {
        #assert(false);
    }

    return true;
}

#if OS == .MACOS || OS == .WINDOWS {
    // User event ids aren't assigned by the kernel on these platforms; we
    // hand them out from a monotonic counter guarded by this mutex.
    next_user_event_id_mutex: Mutex;
    next_user_event_id: User_Event_Id;
}

#if OS == .LINUX {
    create_signal_handle :: (signal: s32) -> handle: s32, success: bool {
        sig_mask: sigset_t;
        sigemptyset(*sig_mask);
        sigaddset(*sig_mask, signal);

        handle := signalfd(-1, *sig_mask, xx SFD.NONBLOCK);
        if handle == -1 {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not create signal handle for signal %: % %", signal, error_code, error_string);
            return -1, false;
        }

        return handle, true;
    }

    #import "Linux";
} else #if OS == .MACOS {
    #import "macos";
} else #if OS == .WINDOWS {
    // Windows backend helpers.

    Timer_Entry :: struct {
        id:          User_Event_Id;
        deadline_ms: s64;           // absolute monotonic ms
        interval_ms: s64;           // only meaningful when recurring
        recurring:   bool;
    }

    _NowMs :: () -> s64 {
        // Apollo_Time-based monotonic clock; we just need deltas, not
        // wall-clock accuracy.
        t := current_time_monotonic();
        return to_milliseconds(t);
    }

    _InsertTimerSorted :: (timers: *[..] Timer_Entry, entry: Timer_Entry) {
        // Insertion sort — N is tiny (MCP: typically 0 or 1 timer).
        // Bubble the newly-appended entry leftward until it's ≥ its predecessor.
        // `for < i: A..B` in Jai iterates B, B-1, ..., A inclusive; when count<=1
        // we want zero iterations, so range A=1, B=count-1 collapses to empty.
        array_add(timers, entry);
        for < i: 1..timers.count - 1 {
            if timers.*[i].deadline_ms < timers.*[i - 1].deadline_ms {
                tmp := timers.*[i];
                timers.*[i] = timers.*[i - 1];
                timers.*[i - 1] = tmp;
            } else {
                break;
            }
        }
    }

    _SortTimers :: (timers: *[..] Timer_Entry) {
        // Bubble sort — fine for small N. Keeps us out of stdlib Sort to
        // avoid pulling a sorting module for a 1-timer workload.
        for i: 0..timers.count - 2 {
            for j: 0..timers.count - 2 - i {
                if timers.*[j].deadline_ms > timers.*[j + 1].deadline_ms {
                    tmp := timers.*[j];
                    timers.*[j] = timers.*[j + 1];
                    timers.*[j + 1] = tmp;
                }
            }
        }
    }

    // Maps a Windows CTRL_* constant to the closest POSIX signal number
    // the caller would have registered via add_signals (SIGINT/SIGTERM/etc).
    _CtrlTypeToSignal :: (ctrl_type: u32) -> s32 {
        SIGINT  :: 2;
        SIGTERM :: 15;
        SIGHUP  :: 1;
        if ctrl_type == CTRL_C_EVENT          return SIGINT;
        if ctrl_type == CTRL_BREAK_EVENT      return SIGTERM;
        if ctrl_type == CTRL_CLOSE_EVENT      return SIGTERM;
        if ctrl_type == CTRL_LOGOFF_EVENT     return SIGHUP;
        if ctrl_type == CTRL_SHUTDOWN_EVENT   return SIGTERM;
        return SIGTERM;
    }

    // Single process-wide recipient for console-ctrl events. Multiple
    // wait_groups registering signals isn't supported on Windows — the
    // latest add_signals wins. Hyperserve only uses this for shutdown.
    gSignalRecipient: *Wait_Group;
    gSignalHandlerInstalled: bool;

    _ConsoleCtrlHandlerThunk :: (ctrl_type: u32) -> s32 #c_call {
        // Runs on a Windows-internal thread. Must be short + thread-safe.
        recipient := gSignalRecipient;
        if recipient == null  return 0;   // FALSE: let default handler run

        push_context {
            sig := _CtrlTypeToSignal(ctrl_type);
            lock(*recipient.wakeup_mutex);
            array_add(*recipient.pending_signals, sig);
            unlock(*recipient.wakeup_mutex);

            // Wake the event loop; ignore errors (wakeup will arrive on the
            // next trigger and we don't have a way to surface failure here).
            if recipient.wakeup_send >= 0 {
                b: u8 = 1;
                send(cast(Socket) recipient.wakeup_send, *b, 1, xx 0);
            }
        }
        return 1;   // TRUE: consumed
    }

    _InstallConsoleCtrlHandler :: (group: *Wait_Group) -> bool {
        gSignalRecipient = group;
        if gSignalHandlerInstalled  return true;
        ok := SetConsoleCtrlHandler(_ConsoleCtrlHandlerThunk, 1);
        if ok == 0  return false;
        gSignalHandlerInstalled = true;
        return true;
    }

    _RestoreConsoleCtrlHandler :: () {
        if gSignalHandlerInstalled {
            SetConsoleCtrlHandler(_ConsoleCtrlHandlerThunk, 0);
            gSignalHandlerInstalled = false;
        }
        gSignalRecipient = null;
    }

    // Win32 primitives — declared inline so callers don't need to `#import
    // "Windows"` (which collides with Socket.MSG if ever co-imported).
    _KERNEL32_WG :: #system_library "kernel32";
    SetConsoleCtrlHandler :: (
        HandlerRoutine : #type (ctrl_type: u32) -> s32 #c_call,
        Add            : s32
    ) -> s32 #foreign _KERNEL32_WG;

    CTRL_C_EVENT        :: 0;
    CTRL_BREAK_EVENT    :: 1;
    CTRL_CLOSE_EVENT    :: 2;
    CTRL_LOGOFF_EVENT   :: 5;
    CTRL_SHUTDOWN_EVENT :: 6;
}

#import "Basic";
#import "Hash_Table";
#if OS != .WINDOWS {
    #import "POSIX";
}
#import "Socket";
#import "Thread";
System :: #import "System";
Hash  :: #import "Hash";


