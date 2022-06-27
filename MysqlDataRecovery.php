<?php

class MysqlDataRecovery
{
    protected $tables = '';
    protected $damageTables = '';
    protected $databaseHost = '127.0.0.1';
    protected $databaseDb = 'db';
    protected $databasePort = 3308;
    protected $databaseUserName = 'root';
    protected $databaseUserPassword = 'root';

    protected $oldFilesDir = './old_files/%s.ibd';
    protected $originFilesDir = '.origin_files/%s.ibd';
    protected $newFilesDir = './new_ibd/%s.ibd';

    public function __construct()
    {
        if (!is_array($this->tables)) {
            $this->tables = explode(PHP_EOL, $this->tables);
        }

        if (!is_array($this->damageTables)) {
            $this->damageTables = explode(PHP_EOL, $this->damageTables);
        }
    }

    public function generateTempCreateTablesSql()
    {
        $sql = '';
        foreach ($this->tables as $table) {
            $sql .= "CREATE TABLE `{$table}` (`id` int(11) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;";
        }

        return $sql;
    }

    public function getOriginalCreateTablesSql()
    {
        $sql = '';

        $conn = $this->getConnection();

        foreach ($this->tables as $table) {
            $fetch = $conn->query("show create table {$table};");
            $response = $fetch->fetch();
            $sql .= $response[1] . ';';
        }

        return $sql;
    }

    public function modifyIbdHeaderId()
    {
        $failTables = [];
        foreach ($this->tables as $table) {
            $oldFilesDir = sprintf($this->oldFilesDir, $table);
            $originFilesDir = sprintf($this->originFilesDir, $table);
            $newFilesDir = sprintf($this->newFilesDir, $table);

            $oldHeaderData = file_get_contents($oldFilesDir, false, null, 32, 16);
            $originHeaderData = file_get_contents($originFilesDir, false, null, 32, 16);

            $file = fopen($oldFilesDir, 'rb+');
            $isModifyHeader = true;
            while (!feof($file)) {
                if ($isModifyHeader) {
                    $buffer = fgets($file, 64);
                    if (strpos($buffer, $oldHeaderData)) {
                        $isModifyHeader = false;
                        $buffer = str_replace($oldHeaderData, $originHeaderData, $buffer);
                        echo '修改头部完成: ' . $table . PHP_EOL;
                    }
                } else {
                    $buffer = fgets($file, 1024 * 1024 * 10);
                }

                $newFile = fopen($newFilesDir, 'ab');
                fwrite($newFile, $buffer);
                fclose($newFile);
            }

            echo '写入新文件完成: ' . $table . PHP_EOL;

            $originHeaderData = file_get_contents($originFilesDir, false, null, 32, 16);
            $newHeaderData = file_get_contents($newFilesDir, false, null, 32, 16);
            if ($originHeaderData === $newHeaderData) {
                echo '校验成功: ' . $table . PHP_EOL;
            } else {
                echo '校验失败: ' . $table . PHP_EOL;
                $failTables[] = $table;
            }
            echo PHP_EOL;

            fclose($file);
        }

        echo '校验失败表: ' . implode(',', $failTables);
    }

    public function validateTableData()
    {
        $conn = $this->getConnection();
        foreach ($this->tables as $table) {
            if (!in_array($table, $this->damageTables, true)) {
                try {
                    $fetch = $conn->query("select * from {$table};");
                    $fetch->fetch();
                } catch (\Error $error) {
                    echo $table . '表无法获取,退出程序.';
                    exit;
                }
            }
        }
    }

    private function getConnection()
    {
        $conn = new PDO("mysql:host={$this->databaseHost};dbname={$this->databaseDb};port={$this->databasePort}", $this->databaseUserName, $this->databaseUserPassword);

        return $conn;
    }
}


$mysqlDataRecovery = new MysqlDataRecovery();
//$tempCreateTablesSql = $mysqlDataRecovery->generateTempCreateTablesSql();
//echo $tempCreateTablesSql;
//$originalCreateTablesSql = $mysqlDataRecovery->getOriginalCreateTablesSql();
//echo $originalCreateTablesSql;
//$mysqlDataRecovery->modifyIbdHeaderId();
$mysqlDataRecovery->validateTableData();
