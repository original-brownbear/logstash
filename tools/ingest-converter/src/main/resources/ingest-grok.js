/**
 * Converts Ingest JSON to LS Grok.
 */
function ingest_to_logstash_grok(json) {

    function map_processor(processor) {
        
        function create_hash_field(name, content) {
            return create_field(name, wrap_in_curly(content));
        }
        
        function grok_hash(processor) {
            function create_pattern_definition_hash(definitions) {
                var content = [];
                for (var key in definitions) {
                    if (definitions.hasOwnProperty(key)) {
                        content.push(create_field(quote_string(key), quote_string(definitions[key])));
                    }
                }
                return create_hash_field("pattern_definitions", content);
            }

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
