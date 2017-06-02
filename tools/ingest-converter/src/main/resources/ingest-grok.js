/**
 * Converts Ingest JSON to LS Grok.
 */
function ingest_to_logstash_grok(json) {

    function create_hash_field(name, content) {
        return create_field(name, wrap_in_curly(content));
    }

    /**
     * Converts Ingest/JSON style pattern array to LS pattern array, performing necessary variable
     * name and quote escaping adjustments.
     * @param patterns Pattern Array in JSON formatting
     * @returns {string} Pattern array in LS formatting
     */
    function create_pattern_array(patterns) {
        return "[\n" + patterns.map(dots_to_square_brackets).map(quote_string).join(",\n") + "\n]";
    }

    function create_pattern_definition_hash(definitions) {
        var content = [];
        for (var key in definitions) {
            if (definitions.hasOwnProperty(key)) {
                content.push(create_field(quote_string(key), quote_string(definitions[key])));
            }
        }
        return create_hash_field("pattern_definitions", content);
    }

    /**
     * Fixes indentation in LS string.
     * @param string LS string to fix indentation in, that has no indentation intentionally with
     * all lines starting on a token without preceding spaces.
     * @returns {string} LS string indented by 3 spaces per level
     */
    function fix_indent(string) {

        function indent(string, shifts) {
            return new Array(shifts * 3 + 1).join(" ") + string;
        }

        var lines = string.split("\n");
        var count = 0;
        var i;
        for (i = 0; i < lines.length; ++i) {
            if (lines[i].match(/(\{|\[)$/)) {
                lines[i] = indent(lines[i], count);
                ++count;
            } else if (lines[i].match(/(\}|\])$/)) {
                --count;
                lines[i] = indent(lines[i], count);
            // Only indent line if previous line ended on relevant control char.
            } else if (i > 0 && lines[i - 1].match(/(=>\s+".+"|,|\{|\}|\[|\])$/)) {
                lines[i] = indent(lines[i], count);
            }
        }
        return lines.join("\n");
    }

    function grok_hash(processor) {
        var grok_data = processor["grok"];
        var grok_contents = create_hash_field(
            "match",
            create_field(
                quote_string(grok_data["field"]),
                create_pattern_array(grok_data["patterns"])
            )
        );
        if (grok_data["pattern_definitions"]) {
            grok_contents = join_hash_fields([
                    grok_contents,
                    create_pattern_definition_hash(grok_data["pattern_definitions"])
            ])
        }
        return grok_contents;
    }

    function map_processor (processor) {
        return fix_indent(
            create_hash(
                "filter",
                create_hash(
                    "grok", grok_hash(processor)
                )
            )
        )
    }

    return JSON.parse(json)["processors"].map(map_processor).join("\n\n") + "\n";
}
