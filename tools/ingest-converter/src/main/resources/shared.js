/**
 * Translates the JSON naming pattern (`name.qualifier.sub`) into the LS pattern
 * [name][qualifier][sub] for all applicable tokens in the given string.
 * This function correctly identifies and omits renaming of string literals.
 * @param string to replace naming pattern in
 * @returns {string} with Json naming translated into grok naming
 */
function dots_to_square_brackets(string) {

    function token_dots_to_square_brackets(string) {
        return string.replace(/(\w*)\.(\w*)/g, "$1][$2").replace(/(\w+)}/g, "$1]}")
            .replace(/{(\w+):(\w+)]/g, "{$1:[$2]");
    }

    var literals = string.match(/\(\?:%{.*\|-\)/);
    var i;
    var tokens = [];
    // Copy String before Manipulation
    var right = string;
    if (literals) {
        for (i = 0; i < literals.length; ++i) {
            var parts = right.split(literals[i], 2);
            right = parts[1];
            tokens.push(token_dots_to_square_brackets(parts[0]));
            tokens.push(literals[i]);
        }
    }
    tokens.push(token_dots_to_square_brackets(right));
    return tokens.join("");
}

function quote_string(string) {
    return "\"" + string.replace(/"/g, "\\\"") + "\"";
}

function wrap_in_curly(string) {
    return "{\n" + string + "\n}";
}

function create_field(name, content) {
    return name + " => " + content;
}

function create_hash(name, content) {
    return name + " " + wrap_in_curly(content);
}

/**
 * All hash fields in LS start on a new line.
 * @param fields Array of Strings of Serialized Hash Fields
 * @returns {string} Joined Serialization of Hash Fields
 */
function join_hash_fields(fields) {
    return fields.join("\n");
}
