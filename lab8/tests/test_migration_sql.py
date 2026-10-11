import pytest

from lab8.migrations import split_sql


def test_semicolons_in_comments_literals_and_identifiers_stay_in_statements():
    first = '''-- Deployment; do not split this comment
CREATE TABLE `semi;colon` (value STRING COMMENT 'a; b', "another;name" INT)
/* outer; /* nested; */ still a comment; */'''
    second = "INSERT INTO `semi;colon` VALUES ('it''s; fine', 1)"
    assert split_sql(first + ';\n' + second + '; -- trailing; comment') == [first, second]


def test_escaped_quotes_and_multiline_dollar_literal():
    sql = r"SELECT 'escaped\';semicolon', \"quoted;identifier\"".replace(r'\"', '"')
    dollar = "SELECT $$line one;\n'line two';$$"
    assert split_sql(sql + ';' + dollar) == [sql, dollar]


@pytest.mark.parametrize('sql', ["SELECT 'unfinished;", 'SELECT "unfinished;',
                                  'SELECT `unfinished;', 'SELECT $$unfinished;',
                                  'SELECT 1; /* unfinished;'])
def test_unclosed_tokens_fail_before_any_execution(sql):
    with pytest.raises(ValueError, match='Unclosed'):
        split_sql(sql)


def test_empty_statements_and_comments_are_not_executed():
    assert split_sql('; -- comment;\n /* nested /* ; */ ; */ ;') == []
    assert split_sql('SELECT 1; SELECT 2 -- end;') == ['SELECT 1', 'SELECT 2 -- end;']
