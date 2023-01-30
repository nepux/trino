/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.trino.plugin.deltalake.expression;

import org.testng.annotations.Test;

import static io.trino.plugin.deltalake.expression.SparkExpressions.createExpression;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestStringLiteral
{
    @Test
    public void testStringLiteral()
    {
        assertStringLiteral("''", "");
        assertStringLiteral("'abc'", "abc");
        assertStringLiteral("'NULL'", "NULL");

        assertStringLiteral("'あ'", "あ");
        assertStringLiteral("'\\u3042'", "あ");
        assertStringLiteral("'👍'", "👍");
        assertStringLiteral("'\\U0001F44D'", "👍");

        assertStringLiteral("'a''quote'", "a'quote");
        assertStringLiteral("\"double-quote\"", "double-quote");
        assertStringLiteral("\"a\"\"double-quote\"", "a\"double-quote");
    }

    @Test
    public void testUnsupportedStringLiteral()
    {
        assertParseFailure("r'raw literal'");
        assertParseFailure("r\"'\\n' represents dnewline character.\"");

        // Spark allows spaces after 'r' for raw literals
        assertParseFailure("r 'a space after prefix'");
        assertParseFailure("r  'two spaces after prefix'");
    }

    private static void assertStringLiteral(String sparkExpression, String expected)
    {
        SparkExpression expression = createExpression(sparkExpression);
        assertEquals(expression, new StringLiteral(expected));
    }

    private static void assertParseFailure(String sparkExpression)
    {
        assertThatThrownBy(() -> createExpression(sparkExpression))
                .hasMessageContaining("Cannot parse Spark expression");
    }
}
