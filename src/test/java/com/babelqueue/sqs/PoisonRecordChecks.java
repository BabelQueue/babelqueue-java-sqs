package com.babelqueue.sqs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashSet;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * The assertions of the core-scope {@code poison_dlq} conformance section (ADR-0035), applied to the DLQ record a
 * consumer actually wrote: the {@code dead_letter} block says {@code poison} / {@code orders}, and the rest of the
 * record is the wrapper of the case or a byte-level splice of the body.
 */
final class PoisonRecordChecks {

    private PoisonRecordChecks() {
    }

    /** The exact bytes of the case's body ({@code body} text or {@code body_base64}). */
    static byte[] bodyOf(JSONObject testCase) {
        return testCase.has("body_base64")
            ? Base64.getDecoder().decode(testCase.getString("body_base64"))
            : testCase.getString("body").getBytes(StandardCharsets.UTF_8);
    }

    static void check(JSONObject testCase, String record) {
        String name = testCase.getString("name");
        JSONObject expect = testCase.getJSONObject("expect");
        String shape = expect.getString("shape");
        JSONObject want = expect.getJSONObject("record");
        assertFalse(record.indexOf('�') >= 0, name + ": the record holds U+FFFD");
        Set<String> keys = topLevelKeys(record);
        boolean wrapper = want.has("raw") && keys.contains("raw");

        switch (shape) {
            case "wrapper" -> assertTrue(wrapper, name + ": expected the wrapper, got " + record);
            case "splice" -> {
                assertFalse(keys.contains("raw_encoding"), name + ": expected a splice, got " + record);
                assertTrue(keys.contains("dead_letter"), name + ": the splice lost dead_letter: " + record);
            }
            case "splice_or_wrapper" -> { }
            default -> throw new AssertionError(name + ": unknown shape " + shape);
        }

        if (wrapper) {
            JSONObject parsed = new JSONObject(record);
            JSONObject deadLetter = (JSONObject) parsed.remove("dead_letter");
            assertEquals("poison", deadLetter.getString("reason"), name);
            assertEquals("orders", deadLetter.getString("original_queue"), name);
            assertTrue(want.similar(parsed), name + ": " + record);
            return;
        }

        assertFalse(keys.contains("raw") && keys.contains("raw_encoding"), name + ": a splice must not wrap");
        assertFalse(keys.contains("raw_encoding"), name + ": a splice must not carry raw_encoding");
        if (expect.has("splice_keys")) {
            Set<String> expectedKeys = new LinkedHashSet<>();
            JSONArray list = expect.getJSONArray("splice_keys");
            for (int i = 0; i < list.length(); i++) {
                expectedKeys.add(list.getString(i));
            }
            Set<String> actualKeys = new LinkedHashSet<>(keys);
            actualKeys.remove("dead_letter");
            assertEquals(expectedKeys, actualKeys, name + ": top-level member names of " + record);
        }
        if (expect.has("splice_literals")) {
            JSONArray literals = expect.getJSONArray("splice_literals");
            for (int i = 0; i < literals.length(); i++) {
                assertTrue(record.contains(literals.getString(i)), name + ": missing literal " + literals.getString(i));
            }
            assertEquals("poison", deadLetterReason(record), name);
        } else {
            JSONObject parsed = new JSONObject(record);
            JSONObject deadLetter = (JSONObject) parsed.remove("dead_letter");
            assertEquals("poison", deadLetter.getString("reason"), name);
            assertEquals("orders", deadLetter.getString("original_queue"), name);
            assertTrue(want.similar(parsed), name + ": " + record);
        }
    }

    /** The {@code reason} of the final {@code dead_letter} member, read from the record tail the splice appends. */
    private static String deadLetterReason(String record) {
        int at = record.lastIndexOf("\"dead_letter\":");
        JSONObject block = new JSONObject(new org.json.JSONTokener(record.substring(at + "\"dead_letter\":".length())));
        assertEquals("orders", block.getString("original_queue"));
        return block.getString("reason");
    }

    /**
     * The distinct top-level member names of a JSON object text, read with a tolerant scanner (string-aware, depth
     * tracking) because a conforming splice may hold values the test's own parser cannot represent.
     */
    static Set<String> topLevelKeys(String json) {
        Set<String> keys = new LinkedHashSet<>();
        int depth = 0;
        int i = 0;
        int n = json.length();
        while (i < n) {
            char ch = json.charAt(i);
            if (ch == '"') {
                int start = i++;
                while (i < n && json.charAt(i) != '"') {
                    i += json.charAt(i) == '\\' ? 2 : 1;
                }
                int end = i++;
                if (depth == 1) {
                    int j = i;
                    while (j < n && " \t\r\n".indexOf(json.charAt(j)) >= 0) {
                        j++;
                    }
                    if (j < n && json.charAt(j) == ':') {
                        keys.add(new JSONArray("[" + json.substring(start, end + 1) + "]").getString(0));
                    }
                }
                continue;
            }
            if (ch == '{' || ch == '[') {
                depth++;
            } else if (ch == '}' || ch == ']') {
                depth--;
            }
            i++;
        }
        return keys;
    }
}
