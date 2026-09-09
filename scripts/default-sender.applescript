-- UI scripting for Messages' app-wide default; never sends a message.
on normalizedAddress(rawValue)
    set resultValue to ""
    repeat with characterValue in characters of (rawValue as text)
        if characterValue is not in {" ", "(", ")", "-", ".", tab, return, linefeed} then set resultValue to resultValue & characterValue
    end repeat
    -- Preserve dots in email addresses.
    if rawValue contains "@" then return rawValue as text
    return resultValue
end normalizedAddress

on run argv
    set requestedAddress to item 1 of argv
    tell application "System Events"
        if not UI elements enabled then error "Enable Accessibility for Blueski to control Messages settings" number 19001
    end tell
    tell application "Messages" to activate
    tell application "System Events"
        tell process "Messages"
            set frontmost to true
            keystroke "," using command down
            set preferencesWindow to missing value
            repeat 30 times
                repeat with candidateWindow in windows
                    try
                        if value of attribute "AXIdentifier" of candidateWindow is "MessagesPreferencesWindow" then set preferencesWindow to candidateWindow
                    end try
                end repeat
                if preferencesWindow is not missing value then exit repeat
                delay 0.1
            end repeat
            if preferencesWindow is missing value then error "Messages settings window not found" number 19003
            click button "iMessage" of toolbar 1 of preferencesWindow
            set senderControl to missing value
            repeat 30 times
                set controls to {}
                repeat with candidateControl in entire contents of preferencesWindow
                    if role of candidateControl is "AXPopUpButton" then set end of controls to contents of candidateControl
                end repeat
                if (count of controls) is 1 then
                    set senderControl to item 1 of controls
                    exit repeat
                end if
                delay 0.1
            end repeat
            if senderControl is missing value then error "Could not uniquely identify the default-sender dropdown" number 19003
            click senderControl
            try
                repeat 30 times
                    if exists menu 1 of senderControl then exit repeat
                    delay 0.1
                end repeat
                set availableAddresses to {}
                set targetItem to missing value
                set matchCount to 0
                repeat with candidateItem in menu items of menu 1 of senderControl
                    set itemAddress to my normalizedAddress(name of candidateItem)
                    set end of availableAddresses to itemAddress
                    ignoring case
                        if requestedAddress is not "" and itemAddress is requestedAddress then
                            set targetItem to contents of candidateItem
                            set matchCount to matchCount + 1
                        end if
                    end ignoring
                end repeat
                if requestedAddress is "" then
                    key code 53
                else
                    if matchCount is not 1 then error "Requested sender is not uniquely available in Messages settings" number 19002
                    click targetItem
                end if
            on error errorMessage number errorNumber
                key code 53
                error errorMessage number errorNumber
            end try
            set selectedAddress to my normalizedAddress(value of senderControl)
            if requestedAddress is not "" then
                ignoring case
                    if selectedAddress is not requestedAddress then error "Messages did not confirm the requested default sender" number 19003
                end ignoring
            end if
            set AppleScript's text item delimiters to linefeed
            return ({selectedAddress} & availableAddresses) as text
        end tell
    end tell
end run
