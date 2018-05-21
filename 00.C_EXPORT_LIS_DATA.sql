SET DEFINE OFF;
CREATE OR REPLACE LUA SCRIPT "C_EXPORT_LIS_DATA" () as
/*
*	Logs events into cis_lis database, sp_Log table
*	Required fields - 'message'
*	If no params passed then 'NA' status will be print in every column 
*/
function proc_Log(message, status, tablename, dbname, procname)
	local query_text = [[
		EXPORT (
			SELECT 
			CURRENT_TIMESTAMP AS dt,
			'Exasol' as spid, 				/* SPID */
			:status as st, 					/* STATE */
			:procname as procname, 			/* Procedure name */
			'Exasol user' as username, 		/* User name */
			'Exasol hostname' as hostname, 	/* Host name */
			:dbname as dbname, 				/* Database name */
			:tablename as tablename, 		/* Table name */
			:msg as msg 					/* Message */
		) INTO JDBC
		AT CIS_LIS
		TABLE [dbo].[sp_Log]
		;
		]]
		
		local res = query(query_text, {
			status			= status or 'NA', 
			procname		= procname or 'NA', 
			dbname			= dbname or 'NA', 
			tablename		= tablename or 'NA',
			msg				= message or 'NA'
		})
end

/*
************************************************************************************************
*
* CLASS STARTS HERE
*
************************************************************************************************
*/
cExportLisData = {}
function cExportLisData:new(filename, ftp_path, ftp)

    /* private parameters */
	local private = {}
		private.dates_in_dir = {}

		function private.create_url(ftp)
			local full_url = ''
			if(not ftp.urlparams) then
				full_url = ftp.url
			elseif (type(ftp.urlparams) == 'string') then
				full_url = ftp.urlparams .. ',' .. ftp.url
			else
				ftp.urlparams = table.concat(ftp.urlparams, ',') .. ','
				full_url = ftp.urlparams .. ftp.url
			end
			return full_url
		end

	/* public parameters */
    local public = {}
		public.filename = filename
		public.ftp_path = ftp_path
		public.ftp_url = private.create_url(ftp)
		public.ftp_user = ftp.user
		public.ftp_password = ftp.password
		public.iterations = 3

    /*
	*	Make current timestamp or add/substract days from given timestamp string
	*
	*	Parameters: cur_date varchar(), days_add decimal(18,0)
	* 	Returns: varchar() in format 'DDMMYYYY'
	*
	*	If no parameters passed then return current date in DDMMYYYY format
	*	Else return given date + days to add
	*
	*	@TODO add date_format as a parameter
	*/
    function public:make_timestamp(cur_date, days_add)
		local date_format = 'DDMMYYYY'
	    if cur_date == nil then
			--return query([[select to_char(curdate(), :df)]],{df=date_format})[1][1] -- return current date
			return self.get_date()
		end
		local real_date = query([[select to_date(:cur_date, :date_format)]],{date_format=date_format, cur_date=cur_date})[1][1]
		return query([[select to_char(add_days(:real_date, :days_add), :date_format)]],{days_add=days_add, real_date=real_date, date_format=date_format})[1][1] -- substring day from given date
	end

	function public:get_date()
		local date_format = 'DDMMYYYY'
		return query([[select to_char(curdate(), :df)]],{df=date_format})[1][1] -- return current date in char
	end

	/*
	*	Searches the array for a given value and returns the first corresponding key if successful
	*	Searches haystack for needle
	*	
	*	Returns the key for needle if it is found in the array, FALSE otherwise.
	*	If needle is found in haystack more than once, the first matching key is returned.
	*	https://stackoverflow.com/questions/33510736/check-if-array-contains-specific-value
	*/
	local function has_value (haystack, needle)
	    for index, value in ipairs(haystack) do
	        if value == needle then
	            return true
	        end
	    end
	
	    return false
	end

	/*
	*	Get all file names from current ftp folder in format like DDMMYYYY
	*
	*	Read files list from folder
	*	https://www.exasol.com/portal/questions/8028744/can-i-import-multiple-csv-files-form-directory-into-table
	*
	*	DEPRECATED!
	*/
	function private:file_dates()
		local query_text = [[
				select substring(v, -12, 8) file_date
				from (
					import into (v varchar(100))
					from csv
					at :ftp_url
					USER :ftp_user IDENTIFIED BY :ftp_password
					FILE :ftp_path
					)
				where v not in ('.', '..');
			]]
		local res = query(query_text, {
			ftp_path		= public.ftp_path, 
			ftp_url			= public.ftp_url, 
			ftp_user		= public.ftp_user, 
			ftp_password	= public.ftp_password
		})
		local res_table = {}
		for i = 1, #res do
			res_table[i] = res[i][1]
		end
		return res_table
		-- res[1][1]
	end

/*
	*	Get all file names from current ftp folder
	*
	*	Read files list from folder
	*	https://www.exasol.com/portal/questions/8028744/can-i-import-multiple-csv-files-form-directory-into-table
	*
	*/
	function private:file_names()
		local query_text = [[
				select v file_name
				from (
					import into (v varchar(250))
					from csv
					at :ftp_url
					USER :ftp_user IDENTIFIED BY :ftp_password
					FILE :ftp_path
					)
				where v not in ('.', '..');
			]]
		local res = query(query_text, {
			ftp_path		= public.ftp_path, 
			ftp_url			= public.ftp_url, 
			ftp_user		= public.ftp_user, 
			ftp_password	= public.ftp_password
		})
		local res_table = {}
		for i = 1, #res do
			res_table[i] = res[i][1]
		end
		return res_table
		-- res[1][1]
	end

	/*
	*	Do the job of finding date_part in 'DDMMYYYY' format in file names into given folder
	*	
	*	Returns FALSE if there is no such filename with given date
	*	Returns TRUE otherwise
	*
	*	DEPRECATED!
	*/
	function public:filename_part_search(date_chunk)
		local search_result = has_value(private.file_dates(), date_chunk)

		if search_result == false then
			return false
		end

		return date_chunk
	end

/*
	*	Do the job of finding filename in file names list from the given folder
	*	
	*	Returns FALSE if there is no such filename
	*	Returns TRUE otherwise
	*/
	function public:find_by_filename(filename)
		local search_result = has_value(private.file_names(), filename)

		if search_result == false then
			return false
		end

		return filename
	end
	
	/*
	*	Runs query (query_text) to get data from file with given source filename (src_filename)
	*/
	function public:export_query(src_filename, query_text)
		local full_file_path = public.ftp_path .. src_filename
		query(query_text, {
					filename		= src_filename, 
					full_file_path	= full_file_path, 
					ftp_user		= public.ftp_user, 
					ftp_password	= public.ftp_password, 
					ftp_url			= public.ftp_url
				})
	end

    /* pure magic here */
    setmetatable(public, self)
    self.__index = self; return public
end
/
