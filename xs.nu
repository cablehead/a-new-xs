alias and-then = if ($in | is-not-empty)
alias ? = if ($in | is-not-empty) { $in }
alias ?? = ? else { return }

def build-query [params] {
    $params | columns | each { |x|
        let value = ($params | get $x)
        let key = $x | str replace "_" "-"
        match ( $value | describe ) {
            "string" => $"($key)=($value)",
            "int" => $"($key)=($value)",
            "bool" => (if $value { $key }),
        }
    } | and-then { $"?($in | str join "&")" }
}

export def _cat [ store: string, flags: record ] {
    let path = "/"
    let query = ( build-query $flags )
    let url = $"localhost($path)($query)"
    curl -v -sN --unix-socket $"($store)/sock" $url | lines | each { |x| $x | from json }
    print $env.LAST_EXIT_CODE
}

export def cat [
    store: string
    --last-id: any
    --follow: any
    --tail
] {
    let follow = (
        match ($follow | describe) {
            "nothing" => false,
            "bool" => $follow,
            "int" => ($follow | into int),
            _ => true,
        }
    )
    _cat $store { last_id: $last_id, follow: $follow, tail: $tail }
}

export def chomp [
    store: string
    chomper: closure
] {
    cat $store
        | insert content {|x| cas ./store $x.hash | from json}
        | each while { |x|
            try { do $chomper $x ; [] } catch {|e| print $"HALT: ($e.msg)" ($x | table -e)}
        } | flatten
}

export def process [
    store: string
    callback: closure
] {
    each {|meta| cas $store $meta.hash | do $callback $meta}
}

export def stream-get [
    store: string
    id: string
] {
    let url = $"localhost/($id)"
    curl -sN --unix-socket $"($store)/sock" $url | from json
}

export def append [
    store: string
    topic: string
    --meta: record
] {
    curl -s -T - -X POST ...(
        $meta | and-then {
            ["-H" $"xs-meta: ($meta | to json -r)"]
        } | default []
    ) --unix-socket $"($store)/sock" $"localhost(if ($topic | str starts-with '/') { $topic } else { $"/($topic)" })"
}

export def cas [
    store: string
    hash: string
] {
    curl -sN --unix-socket $"($store)/sock" $"localhost/cas/($hash)"
}
