#!/usr/bin/php
<?php

require_once('cdr_config.php');
require_once('cdr_libs.php');

$debug = 'stdout';

// Fix ups for CDR field names
if (count($_config['cdr_fields']['v18']['names']) != $_config['cdr_fields']['v18']['count']) {
    die("Error in number of fields");
}
array_walk($_config['cdr_fields']['v18']['names'], function (&$n) { $n = trim($n); });
array_walk($_config['cdr_fields']['v18']['names'], function (&$n) { $n = str_replace('.','_',$n); });
// End fixups

if (is_dir($_config['cdr']['path']) === false) {
    logger("CDR path not found {$_config['cdr']['path']}\n",'err',$debug);
    exit(1);
}

//DO CLEAN UP OF OLD CDR's
logger("Starting Cleanup of unneeded CDR's",null,$debug);
$cdr_path = dir($_config['cdr']['path']);
while(false !== ($bs_dir = $cdr_path->read())) {
    if (($bs_dir == '.')||($bs_dir == '..')) {
        continue;
    }
    logger("Scanning {$bs_dir}",null,$debug);

    $archives = array();
    $not_archives = array();
    $merged_list = array();

    // Cleanup archive files
    if (is_dir("{$_config['cdr']['path']}/{$bs_dir}/archive") === true) {
        $archive_path = dir("{$_config['cdr']['path']}/{$bs_dir}/archive");
        while(false !== ($cdr_file = $archive_path->read())) {
            if (($cdr_file == '.')||($cdr_file == '..')) {
                continue;
            }

            if (substr($cdr_file,-2) == 'gz') {
                $archives[] = $cdr_file;
                $trimmed_name = substr($cdr_file,0,-3);
                $seq = substr(end(explode('-',$trimmed_name)),0,-4);
                $merged_list[$seq] = $trimmed_name;
                unset($trimmed_name);
            } else {
                $not_archives[] = $cdr_file;
                $seq = substr(end(explode('-',$cdr_file)),0,-4);
                $merged_list[$seq] = $cdr_file;
            }
        }
        foreach ($archives as $archive) {
            if (in_array(substr($archive,0,-3),$not_archives) === true) {
                logger("Cleaning up archive " . substr($archive,0,-3),null,$debug);
                if (is_writeable("{$_config['cdr']['path']}/{$bs_dir}/archive/" . substr($archive,0,-3)) === true) {
                    unlink("{$_config['cdr']['path']}/{$bs_dir}/archive/" . substr($archive,0,-3));
                } else {
                    logger("Cannot remove {$_config['cdr']['path']}/{$bs_dir}/archive/" . substr($archive,0,-3),null,$debug);
                } 
            }
        }
    } else {
        logger("No archive dir for {$bs_dir}",'err',$debug);
    }

    // Cleanup active files
    if (is_dir("{$_config['cdr']['path']}/{$bs_dir}/active") === true) {
        $active_path = dir("{$_config['cdr']['path']}/{$bs_dir}/active");
        while(false !== ($cdr_file = $active_path->read())) {
            if (($cdr_file == '.')||($cdr_file == '..')) {
                continue;
            }
            $seq = substr(end(explode('-',$cdr_file)),0,-4);
            if (isset($merged_list[$seq]) === true) {
                logger("Cleaning up active {$cdr_file}",null,$debug);
                if (is_writeable("{$_config['cdr']['path']}/{$bs_dir}/active/{$cdr_file}") === true) {
                    unlink("{$_config['cdr']['path']}/{$bs_dir}/active/{$cdr_file}");
                } else {
                    logger("Cannot remove {$_config['cdr']['path']}/{$bs_dir}/active/{$cdr_file}",null,$debug);
                }
            }
        }
    }

    unset($archives, $not_archives, $merged_list);
}
logger("Ending Cleanup of unneeded CDR's",null,$debug);
unset($cdr_path);


logger("Starting import of CDR data",null,$debug);
$cdr_path = dir($_config['cdr']['path']);
while(false !== ($entry = $cdr_path->read())) {
    if (($entry == '.')||($entry == '..')) {
        continue;
    }
    logger("Processing archives under {$entry}",null,$debug);
    $import = find_archive_needing_import("{$_config['cdr']['path']}/{$entry}",$_config);
    foreach ($import as $file_name) {
        import_cdr("{$file_name}",$_config);
    }

    logger("Processing active under {$entry}",null,$debug);
    $active_path = dir("{$_config['cdr']['path']}/{$entry}/active");
    while(false !== ($active_entry = $active_path->read())) {
        if (($active_entry == '.')||($active_entry == '..')) {
            continue;
        }
        if (substr($active_entry,-4) == '.csv') {
            import_cdr("{$_config['cdr']['path']}/{$entry}/active/{$active_entry}",$_config,false);
        }
    }
}



?>
