"""Append-only, checksummed SQL migrations. Never edit an applied migration."""
from hashlib import sha256


def split_sql(content):
    """Split ordinary SQL statements outside quotes and nested SQL comments.

    Supports Spark quoted strings/identifiers, backslash and doubled-quote
    escapes, and dollar-quoted literals. Compound SQL scripting blocks are
    outside this migration format; put procedures in a separate executor.
    Parse the whole file before executing anything, rejecting unclosed tokens.
    """
    statements, buffer = [], []
    quote = None
    block_depth = 0
    line_comment = False
    has_sql = False
    index = 0
    while index < len(content):
        char = content[index]
        pair = content[index:index + 2]
        if line_comment:
            buffer.append(char)
            line_comment = char not in '\r\n'
        elif block_depth:
            if pair in ('/*', '*/'):
                block_depth += 1 if pair == '/*' else -1
                buffer.append(pair)
                index += 1
            else:
                buffer.append(char)
        elif quote:
            if content.startswith(quote, index):
                buffer.append(quote)
                index += len(quote) - 1
                if len(quote) == 1 and content[index + 1:index + 2] == quote:
                    buffer.append(quote)
                    index += 1
                else:
                    quote = None
            elif char == '\\' and len(quote) == 1 and index + 1 < len(content):
                buffer.append(content[index:index + 2])
                index += 1
            else:
                buffer.append(char)
        elif pair in ('--', '/*'):
            line_comment = pair == '--'
            block_depth = int(pair == '/*')
            buffer.append(pair)
            index += 1
        elif char in "'\"`":
            quote = char
            has_sql = True
            buffer.append(char)
        elif pair == '$$':
            quote = '$$'
            has_sql = True
            buffer.append(pair)
            index += 1
        elif char == ';':
            if has_sql:
                statements.append(''.join(buffer).strip())
            buffer, has_sql = [], False
        else:
            has_sql = has_sql or not char.isspace()
            buffer.append(char)
        index += 1
    if quote or block_depth:
        raise ValueError('Unclosed SQL quote or block comment in migration')
    if has_sql:
        statements.append(''.join(buffer).strip())
    return statements


def pending_migrations(files, applied):
    planned = []
    available = {path.name for path in files}
    if set(applied) - available:
        raise ValueError("An applied migration has been removed from Git")
    for path in sorted(files):
        content = path.read_text()
        digest = sha256(content.encode()).hexdigest()
        if path.name in applied:
            if applied[path.name] != digest:
                raise ValueError(f"Applied migration changed: {path.name}")
        else:
            planned.append((path.name, digest, content))
    return planned
