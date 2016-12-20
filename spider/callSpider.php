#!/usr/bin/php
<?php

## 25-05-14 RJ 0.00 Script to access Interface Mechanism APIs
## 02-06-14 RJ 0.01 Tidied up somewhat
## 05-05-15 RJ 0.02 Switched to 'simplexml', re-added DescribeFunction() and Login() and commented

$g_Host = "devsys";			# Address of the server (usually 'pm')
$g_ProductVersion = "1.00";	# The version of our software
$g_Session = Array();		# Global session (->id, ->token)

$g_Practices = Array();		# Array of practice information keyed on code
							# ->nacs, ->practiceName, ->type (Main or Branch)

function CallApi($api, $params, $input, &$result)
# Calls an API, returning a result code of...
# 0		Call worked (although API/Spider may have returned an error)
# -1	Curl error ($result will contain details)
# 1...	HTTP response code
# $result gets the response returned from spider
{
	global $g_Host;
	global $g_ProductVersion;
	global $g_Session;

# print "CallApi($api, $sessionId, $sessionToken, $params, $input, &result)\n";
	$sessionId = @$g_Session['id'];
	$sessionToken = @$g_Session['token'];
	$url = "https://$g_Host:4510/rpc/$api";
	if ($params) {
		$paramArray = array();
		foreach ($params as $key => $value) {
			array_push($paramArray, urlencode($key)."=".urlencode($value));
		}
		if (count($paramArray))
			$url .= '?'.implode("&", $paramArray);
	}

	$ch = curl_init($url);
	curl_setopt($ch, CURLOPT_SSLCERT, "client.cabundle.pem");
	curl_setopt($ch, CURLOPT_CAINFO, "client.cabundle.pem");
	curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, false);
	curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
	curl_setopt($ch, CURLOPT_HTTPHEADER, array("Content-type: text/xml",
			"X-MTRPC-SESSION-ID: $sessionId",
			"X-MTRPC-SESSION-TOKEN: $sessionToken",
			"X-MTRPC-PRODUCT-VERSION: $g_ProductVersion",
			));
	curl_setopt($ch, CURLOPT_POSTFIELDS, $input);
	curl_setopt($ch, CURLOPT_HEADER, false);		# Set true to include header in output for testing

	$result = curl_exec($ch);
	if (curl_errno($ch)) {
		$result=sprintf("Curl error %d: %s", curl_errno($ch), curl_error($ch));
		$err=-1;
	} else {
		$err = curl_getinfo($ch, CURLINFO_HTTP_CODE);
		if ($err == 200) $err=0;
	}

	return $err;
}

function SessionCreate()
# Uses the session.create API to create a session, also populating the global array of practices
# Sets	$g_Session - used by CallApi()
# Sets	$g_Practices - Array of practices on the server (including branches)
{
	global $g_Session;
	global $g_Practices;

	$err = CallApi("session.create", "", "", $output);
	if (!$err) {
		$xml = simplexml_load_string($output);
		$sess = $xml->result->{'session.create'}[0];
		$attrs=(array)$sess->attributes();
		$id=$attrs['@attributes']['sessionId'];
		$token=$attrs['@attributes']['sessionToken'];

		$practices = $sess->practiceSite;
		$g_Practices = array();
		foreach ($practices as $practice) {
			$code = false;

			foreach ($practice->attributes() as $name => $thing) {
				$value = (string)$thing;

				if ($name == 'nacs') $s->nacs=$value;
				elseif ($name == 'practiceName') $s->name=$value;
				elseif ($name == 'type') $s->type=$value;
				elseif ($name == 'code') $code=$value;
			}
			if ($code !== false)
				$g_Practices[$code]=$s;
		}
		$g_Session = array("id"=>$id,"token"=>$token);
	} else {
		print("SessionCreate error - $output\n");
		$g_Session = false;
	}

	return $g_Session;
}
 
function Login($practice,$user,$pass)
# Logs in as a given user to the practice
# Returns result as a string for the caller to interpret.
{
	CallApi("session.authenticate", array('login'=>$user,'password'=>$pass,'practiceSiteCode'=>$practice),"", $output);

	return $output;
}

function DescribeApi($function)
# Describes any API.  The result is dependant on the implementation of the API, but should be consistent.
{
    $err = CallApi("spider.adl", array('function'=>$function), "", $output);
	return $err ? $err : $output;
}

#main()

if (SessionCreate()) {
	$sites = array_keys($g_Practices);			# Get practice codes (the keys to the $g_Practices array)
	$practice=$sites[0];						# Use the first (may not be correct in real life, but just for a test)
	$login = Login($practice, "pm1", "pm1");	# Log us in
	print "Result from logging in:\n";
	print $login;
	print "\n";

	print "Result of bulletin.get call:\n";
	$err = CallApi("bulletin.get", array('user'=>'test'), "", $output);
	print_r($output);
	print "\n";

	print "Description of doc.save:\n";
	$func = DescribeApi("doc.save");
	print_r($func);
	print "\n";

	exit(0);
} else {
	echo "Unable to create session\n";
}
exit(0);

?>
