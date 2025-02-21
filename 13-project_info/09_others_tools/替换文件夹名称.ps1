$newPrefix = "对公组"
Get-ChildItem -Directory | ForEach-Object {
    $oldName = $_.Name 
    if($oldName -match '^([^_]*)_(.*)') {
        $newName = $newPrefix + '_' + $matches[2]
        Rename-Item -Path $_.FullName -NewName $newName
    }
}