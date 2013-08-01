<?php

function logger($message,$level = null,$output_method = null)
{
    $app_name = end(explode('/',$_SERVER['PHP_SELF']));

    switch ($level) {
        case 'notice':
            if ($output_method == 'stdout') {
                file_put_contents("php://stdout", date("Y-m-d H:i:s") . " {$app_name}[" . getmypid() . "] LOG {$message}\n");
            } else {
                openlog($app_name, LOG_ODELAY | LOG_PID, LOG_USER);
                syslog(LOG_NOTICE, $message);
                closelog();
            }
            break;
        case 'err':
            if ($output_method == 'stdout') {
                file_put_contents("php://stderr", date("Y-m-d H:i:s") . " {$app_name}[" . getmypid() . "] LOG {$message}\n");
            } else {
                openlog($app_name, LOG_ODELAY | LOG_PID, LOG_USER);
                syslog(LOG_ERR, $message);
                closelog();
            }
            break;
        case 'debug':
            if ($output_method == 'stdout') {
                file_put_contents("php://stderr", date("Y-m-d H:i:s") . " {$app_name}[" . getmypid() . "] LOG {$message}\n");
            } else {
                openlog($app_name, LOG_ODELAY | LOG_PID, LOG_USER);
                syslog(LOG_DEBUG, $message);
                closelog();
            }
            break;
        default:
            if ($output_method == 'stdout') {
                file_put_contents("php://stdout", date("Y-m-d H:i:s") . " {$app_name}[" . getmypid() . "] LOG {$message}\n");
            } else {
                openlog($app_name, LOG_ODELAY | LOG_PID, LOG_USER);
                syslog(LOG_INFO, $message);
                closelog();
            }
    }

    return true;
}

function mongo_connect($mongo_config) {
    $mongo = new Mongo($mongo_config['connect'], $mongo_config['options']);
    $mdb =& $mongo->selectDB($mongo_config['database']);
    return $mdb;
}

function find_archive_needing_import($dir,$_config)
{
    $return = array();

    $mdb = mongo_connect($_config['mongo']);

    // Import archive files
    if (is_dir("{$dir}/archive") === true) {
        $archive_path = dir("{$dir}/archive");
        while(false !== ($entry = $archive_path->read())) {
            if (($entry == '.')||($entry == '..')) {
                continue;
            }
            if (substr($entry,-3) == '.gz') {
                $filename = substr($entry,0,-3);
            } else {
                $filename = $entry;
            }

            $res = $mdb->import_log->count(array('filename' => $filename));
            if ($res == 0) {
                $return[] = "{$dir}/archive/{$entry}";
            }
        }
    }

    return $return;
}

function import_cdr($file,$_config)
{
    $version = null;
    $encoding = null;

    $comp_lib = find_compress_lib($file);
    logger("Processing {$file}",null,$_config['debug']);

    $mdb = mongo_connect($_config['mongo']);

    $fh = fopen("{$comp_lib}{$file}",'r');
    $l_n = 0;
    while (($line = fgets($fh)) !== false) {
        if ($l_n == 0) {
            //We're reading the first line
            $tmp = explode(" ",trim($line));
            foreach ($tmp as $t_line) {
                parse_str($t_line);
            }
            $l_n++;
            continue;
        }
        $a_line = v18_process_line($line,$_config);
        if (is_array($a_line) === true) {
            $a_line['cdr_version'] = $version;
            $a_line['cdr_encoding'] = $encoding;
            $mdb->cdr->insert($a_line);
        }
        $l_n++;
    }
    fclose($fh);
    $mdb->cdr->ensureIndex(array('normalized_calledNumber' => 1), array('sparse' => true, 'background' => true));
    $mdb->cdr->ensureIndex(array('normalized_calledNumber' => 1, 'startTime' => 1), array('sparse' => true, 'background' => true));
    $r = record_imported($file,$l_n,$_config);

    logger("Processed {$l_n} lines from {$file}",null,$_config['debug']);
    return true;
}

function record_imported($file,$cnt,$_config)
{
    $mdb = mongo_connect($_config['mongo']);
    //as3/archive/BW-CDR-20130712000000-2-000C2930BC2D-000800.csv
    if (substr($file,-3) == '.gz') {
        $filename = substr(end(explode('/',$file)),0,-3);
    } else {
        $filename = end(explode('/',$file));
    }
    $a = array('filename' => $filename, 'processed_lines' => $cnt);
    $res = $mdb->import_log->insert($a);

}

function v18_process_line($line,$_config)
{
    // BS doesn't quote stuff so we just strip garbage
    $line = str_replace("\,", "", $line);

    $line = str_getcsv($line);
    if (count($line) == $_config['cdr_fields']['v18']['count']) {
        // combine with field names
        $l_line = array_combine($_config['cdr_fields']['v18']['names'], $line);
        // convert recordId to mongo id field
        $l_line['_id'] = $l_line['recordId'];
        unset($l_line['recordId']);
        // convert to mongo timestamps
        $l_line['startTime'] = bwdate_totime($l_line['startTime']);
        $l_line['answerTime'] = bwdate_totime($l_line['answerTime']);
        $l_line['releaseTime'] = bwdate_totime($l_line['releaseTime']);
        $l_line['normalized_calledNumber'] = normalize_calledNumber($l_line['calledNumber']);
        return $l_line;
    }
    if (count($line) == 45) {
        return $line;
    }

    logger("Line length wrong " . count($line) . " fields, ID: {$line[0]}",null,$_config['debug']);
    return false;
}

function normalize_calledNumber($number)
{
    // Strip + at start
    if (substr($number,0,1) == '+') {
        $number = substr($number,1);
    }

    // Return things that look like normal US numbers
    if ((strlen($number) == 11)&&(substr($number,0,1) == '1')) {
        return $number;
    }

    // Seems like a US number, but has to ones in front, strip one
    if ((strlen($number) == 12)&&(substr($number,0,2) == '11')) {
        return substr($number,1);
    }

    // Strip 9 on international calls
    if ((strlen($number) >=14)&&(substr($number,0,4) == '9011')) {
        return substr($number,1);
    }

    // Strip 9 on international calls
    if ((strlen($number) >=14)&&(substr($number,0,4) == '1011')) {
        return substr($number,1);
    }

    // Return international calls
    if ((strlen($number) >=13)&&(substr($number,0,3) == '011')) {
        return $number;
    }

    // fixup 10 digit to 11
    if ((strlen($number) == 10)&(substr($number,0,1) != '1')) {
        return "1{$number}";
    }

    // probably a junk number so null it out so we don't index it; 
    return null;
}

function bwdate_totime($bw_date)
{
    if (strlen($bw_date)!=18) {
        return '';
    }

    $year = substr($bw_date,0,4);
    $month = substr($bw_date,4,2);
    $day = substr($bw_date,6,2);
    $hour = substr($bw_date,8,2);
    $min = substr($bw_date,10,2);
    $sec = substr($bw_date,12,2);
    $usec = substr($bw_date,15,3);

    $output['sec'] = strtotime("{$year}-{$month}-{$day} {$hour}:{$min}:{$sec}");
    $output['usec'] = $usec;
    return new MongoDate($output['sec'], $output['usec']);
}

function find_compress_lib($file)
{
    if (substr($file,-3) == '.gz') {
        return 'compress.zlib://';
    }
    if (substr($file,-4) == '.bz2') {
        return 'compress.bzip2://';
    }
    if (substr($file,-4) == '.txt') {
        return 'file://';
    }
    if (substr($file,-4) == '.csv') {
        return 'file://';
    }
    throw new exception("unknown file type");
    return false;
}

?>
