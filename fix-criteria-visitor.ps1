#!/usr/bin/env pwsh
# Fix JPA Criteria API - remove Visitor references and fix method signatures

# Fix CompoundSelection
$content = Get-Content 'src\main\java\org\junify\db\jpa\criteria\CompoundSelection.java' -Raw
$content = $content -replace 'import jakarta\.persistence\.criteria\.Visitor;', ''
$content = $content -replace '\s+@Override\s+public <X> X accept\(Visitor<X> visitor\) \{\s+return visitor\.visitSelection\(this\);\s+\}', ''
$content | Set-Content 'src\main\java\org\junify\db\jpa\criteria\CompoundSelection.java' -NoNewline
Write-Host "Fixed CompoundSelection.java"

# Fix FunctionImpl
$content = Get-Content 'src\main\java\org\junify\db\jpa\criteria\FunctionImpl.java' -Raw
$content = $content -replace 'import jakarta\.persistence\.criteria\.Visitor;', ''
$content = $content -replace '\s+@Override\s+public <X> X accept\(Visitor<X> visitor\) \{\s+return visitor\.visitExpression\(this\);\s+\}', ''
$content | Set-Content 'src\main\java\org\junify\db\jpa\criteria\FunctionImpl.java' -NoNewline
Write-Host "Fixed FunctionImpl.java"

# Fix JoinImpl
$content = Get-Content 'src\main\java\org\junify\db\jpa\criteria\JoinImpl.java' -Raw
$content = $content -replace 'import jakarta\.persistence\.criteria\.Visitor;', ''
$content = $content -replace '\s+@Override\s+public <V> V accept\(Visitor<V> visitor\) \{\s+return visitor\.visitJoin\(this\);\s+\}', ''
$content | Set-Content 'src\main\java\org\junify\db\jpa\criteria\JoinImpl.java' -NoNewline
Write-Host "Fixed JoinImpl.java"

# Fix LiteralImpl
$content = Get-Content 'src\main\java\org\junify\db\jpa\criteria\LiteralImpl.java' -Raw
$content = $content -replace 'import jakarta\.persistence\.criteria\.Visitor;', ''
$content = $content -replace '\s+@Override\s+public <X> X accept\(Visitor<X> visitor\) \{\s+return visitor\.visitExpression\(this\);\s+\}', ''
$content | Set-Content 'src\main\java\org\junify\db\jpa\criteria\LiteralImpl.java' -NoNewline
Write-Host "Fixed LiteralImpl.java"

# Fix OrderImpl
$content = Get-Content 'src\main\java\org\junify\db\jpa\criteria\OrderImpl.java' -Raw
$content = $content -replace 'import jakarta\.persistence\.criteria\.Visitor;', ''
$content = $content -replace '\s+@Override\s+public <X> X accept\(Visitor<X> visitor\) \{\s+return visitor\.visitOrder\(this\);\s+\}', ''
$content | Set-Content 'src\main\java\org\junify\db\jpa\criteria\OrderImpl.java' -NoNewline
Write-Host "Fixed OrderImpl.java"

# Fix ParameterExpressionImpl
$content = Get-Content 'src\main\java\org\junify\db\jpa\criteria\ParameterExpressionImpl.java' -Raw
$content = $content -replace 'import jakarta\.persistence\.criteria\.Visitor;', ''
$content = $content -replace '\s+@Override\s+public <X> X accept\(Visitor<X> visitor\) \{\s+return visitor\.visitParameterExpression\(this\);\s+\}', ''
$content | Set-Content 'src\main\java\org\junify\db\jpa\criteria\ParameterExpressionImpl.java' -NoNewline
Write-Host "Fixed ParameterExpressionImpl.java"

# Fix PathImpl
$content = Get-Content 'src\main\java\org\junify\db\jpa\criteria\PathImpl.java' -Raw
$content = $content -replace 'import jakarta\.persistence\.criteria\.Visitor;', ''
$content = $content -replace '\s+@Override\s+public <X> X accept\(Visitor<X> visitor\) \{\s+return visitor\.visitPath\(this\);\s+\}', ''
$content | Set-Content 'src\main\java\org\junify\db\jpa\criteria\PathImpl.java' -NoNewline
Write-Host "Fixed PathImpl.java"

# Fix PredicateImpl - remove duplicate getExpressions and Visitor
$content = Get-Content 'src\main\java\org\junify\db\jpa\criteria\PredicateImpl.java' -Raw
$content = $content -replace 'import jakarta\.persistence\.criteria\.Visitor;', ''
$content = $content -replace '\s+@Override\s+public List<Predicate> getExpressions\(\) \{\s+return \(List<Predicate>\) \(List\<?>\) expressions;\s+\}', ''
$content = $content -replace '\s+@Override\s+public <X> X accept\(Visitor<X> visitor\) \{\s+return visitor\.visitPredicate\(this\);\s+\}', ''
$content | Set-Content 'src\main\java\org\junify\db\jpa\criteria\PredicateImpl.java' -NoNewline
Write-Host "Fixed PredicateImpl.java"

# Fix RootImpl
$content = Get-Content 'src\main\java\org\junify\db\jpa\criteria\RootImpl.java' -Raw
$content = $content -replace 'import jakarta\.persistence\.criteria\.Visitor;', ''
$content = $content -replace '\s+@Override\s+public <Y> X accept\(Visitor<X> visitor\) \{\s+return visitor\.visitRoot\(this\);\s+\}', ''
$content | Set-Content 'src\main\java\org\junify\db\jpa\criteria\RootImpl.java' -NoNewline
Write-Host "Fixed RootImpl.java"
