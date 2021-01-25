grammar Tel;

INT : '-'? [0-9]+ ;                 // integer
REAL : '-'? [0-9]+ '.' [0-9]+ ;     // integer
TRUE : 'true' | 'TRUE';             // true
FALSE : 'false' | 'FALSE';          // false
NOT : 'not' | 'NOT';
KW_IS : 'is' | 'IS';
KW_NULL : 'null' | 'NULL';
WORD : [a-zA-Z0-9_.]+;              // one word (either part of slug or fn name)
STRING_CONSTANT : '"' ( '\\"' | ~'"' )* '"' ;    // string constant. Not greedy, and supports \ to escape " char.
SINGLE_QUOTED_ELEMENT: '\'' ( '\\\'' | ~'\'' )* '\'' ;    // string element surrounded by single quotes. Not greedy, and supports \ to escape ' char.

L_BRACKET: '(';
R_BRACKET: ')';
TAXON_NAMESPACE_DELIMITER: '|';
TAXON_TAG_DELIMITER: ':';
FN_PARAMETER_DELIMITER: ',';
// OPERATORS
OR : '||';
AND : '&&';
EQ : '==';
NEQ : '!=';
GT : '>';
LT : '<';
GTEQ : '>=';
LTEQ : '<=';
PLUS : '+';
MINUS : '-';
MULT : '*';
DIV : '/';
OPTIONAL_TAXON_OPERATOR: '?'; // Taxon slug prefix noting, that the taxon slug is optional.


WS : [ \t\r\n]+ -> skip ; // skip spaces, tabs, newlines

// auxiliarly rules
fn : WORD L_BRACKET expr? (FN_PARAMETER_DELIMITER expr)* R_BRACKET ; // matches functions
taxon: WORD (TAXON_NAMESPACE_DELIMITER WORD)? (TAXON_TAG_DELIMITER WORD)? ;  // matches a taxon slug
taxon_expr: OPTIONAL_TAXON_OPERATOR?taxon ;  // taxon slug with optional taxon prefix operator

// final rules
parse: expr EOF; // main rule for parsing

expr
: NOT expr                                                     #notExpr
| expr op=(MULT | DIV) expr                                    #multiplicationExpr
| expr op=(PLUS | MINUS) expr                                  #additiveExpr
| expr op=(OR | AND | EQ | NEQ | GT | LT | GTEQ | LTEQ) expr   #logicalExpr
| expr KW_IS NOT? KW_NULL                                      #nullTestExpr
| atom                                                         #atomExpr
;

atom
:  L_BRACKET expr R_BRACKET  #bracketExpr
| (INT | REAL)              #numberAtom
| fn                        #fnExpr
| (TRUE | FALSE)            #booleanAtom
| taxon_expr                #taxonSlugAtom
| SINGLE_QUOTED_ELEMENT     #singleQuotedAtom
| STRING_CONSTANT           #stringConstantAtom
;
