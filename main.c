#include <mysql.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>

#include "combinations.h"	//As of now this file is in my "Ongoing Projects/Combinations" directory

struct relation
{
	char name[101];
	
	short unsigned int no_attributes;
	char attribute[100][101];
	
	bool primary_key[100];
	
} table[100];

short unsigned int no_tables = 0;

bool get_tables ( const char *, const char *, const char *, const char * );

void disp_table_info ( void );

bool gen_queries_top ( const char *, const char *, const char *, const char *, const char * );

bool gen_query ( MYSQL_ROW, unsigned int, unsigned int, unsigned int, char **, FILE * );

int main ( int argc, char **argv )	//mysql hostname, username, password, database name, result(THE input)
{
	if ( argc != 6 )
	{
	fprintf ( stderr, "Insufficient Arguments" );
	exit ( 1 );
	}

	get_tables ( argv[1], argv[2], argv[3], argv[4] );

	//disp_table_info ();

	gen_queries_top ( argv[1], argv[2], argv[3], argv[4], argv[5] );

	return 0;
}

bool get_tables ( const char *server, const char *username, const char *password, const char *db )
{
	MYSQL *conn;
	MYSQL_RES *res_set;
	MYSQL_ROW row;

	conn = mysql_init ( NULL );

	int status;

	MYSQL *status_mysql;

	status_mysql = mysql_real_connect ( conn, server, username, password, db, 0, NULL, 0 );

	if ( status_mysql == NULL )
	{
		fprintf ( stderr, "%s\n", mysql_error ( conn ) );
		exit ( 1 );
	}

	/*Getting the table names in the database*/

	status = mysql_query ( conn, "SHOW TABLES" );

	if ( status )
	{
		fprintf ( stderr, "%s\n", mysql_error ( conn ) );
	exit ( 1 );
	}    

	res_set = mysql_store_result ( conn );

	no_tables = 0;
	while ( (row = mysql_fetch_row ( res_set ) ) != NULL )
	{
		strcpy ( table[no_tables].name, row[0] );		
		
		no_tables++;
	}

	mysql_free_result ( res_set );

	/*Got the table names*/

	/*Getting the attributes and primary key stats for all the tables*/
	char query[100];

	short unsigned int i;
	for ( i = 0; i < no_tables; i++ )
	{
		strcpy ( query, "DESC " );
		strcat ( query, table[i].name );

		status = mysql_query ( conn, query );

			if ( status )
			{
				fprintf ( stderr, "%s\n", mysql_error ( conn ) );
				exit ( 1 );
			}  
			
			res_set = mysql_store_result ( conn );

		table[i].no_attributes = 0;
		
		while ( (row = mysql_fetch_row ( res_set ) ) != NULL )
		{
			strcpy ( table[i].attribute[table[i].no_attributes], row[0] );
			
			if ( ! strcmp ( row[3], "PRI" ) )
			{
				table[i].primary_key[table[i].no_attributes] = true;
			}
			else
			{
				table[i].primary_key[table[i].no_attributes] = false;
			}
			
			table[i].no_attributes++;
		}

		mysql_free_result ( res_set );
	}

	/*Got the attributes and primary key stats for all the tables*/

	mysql_close ( conn );
	
	mysql_library_end();

	return true;
}

void disp_table_info ( void )
{
	printf ( "No. Tables : %hu", no_tables );
    
    short unsigned int i, j;
    for ( i = 0; i < no_tables; i++ )
    {
    	printf ( "\n\nTable Name : %s", table[i].name );
    	
    	for ( j = 0; j < table[i].no_attributes; j++ )
    	{
    		printf ( "\n\t%s", table[i].attribute[j] );
    		
    		if ( table[i].primary_key[j] == true )
    		{
    			printf ( "\t_P_" );
    		}
    		
    	}
    }
}

bool gen_queries_top ( const char *server, const char *username, const char *password, const char *db, const char *result )
{
	MYSQL *conn;
	MYSQL_RES *res_set;
	MYSQL_ROW row;

	conn = mysql_init ( NULL );

	MYSQL *status_mysql;

	status_mysql = mysql_real_connect ( conn, server, username, password, db, 0, NULL, 0 );

	if ( status_mysql == NULL )
	{
		fprintf ( stderr, "%s\n", mysql_error ( conn ) );
		exit ( 1 );
	}
	
	FILE *fp;
	fp = fopen ( "generated_queries.sql", "w" );
	unsigned int no_combinations;
	char **output = NULL;
	char query[100];
	unsigned int i, j, k;
	for ( i = 0; i < no_tables; i ++ )
	{
		for ( j = 0; j < table[i].no_attributes; j ++ )
		{
			strcpy ( query, "SELECT * FROM " );
			strcat ( query, table[i].name );
			strcat ( query, " WHERE " );
			strcat ( query, table[i].attribute[j] );
			strcat ( query, " = '" );
			strcat ( query, result );
			strcat ( query, "';" );
			
			mysql_query ( conn, query );
			
			res_set = mysql_store_result ( conn );
			
			while ( ( row = mysql_fetch_row ( res_set ) ) != NULL )
			{
				for ( k = 1; k <= table[i].no_attributes; k ++ )
				{
					output = NULL;
					
					no_combinations = combinations ( table[i].no_attributes, k, &output );
					
					gen_query ( row, i, j, no_combinations, output, fp );
					
					combinations_output_deallocate ( no_combinations, output );
				}
			}
			
			mysql_free_result ( res_set );
		}
	}
	
	mysql_close ( conn );
	
	mysql_library_end();	
	
	fprintf ( fp, "\n\n" );
	
	fclose ( fp );
	
	return true;
}

bool gen_query ( MYSQL_ROW row, unsigned int table_no, unsigned int attribute_no, unsigned int no_combinations, char **input , FILE *fp )
{
	char query[1000];
	
	unsigned int i, j;
	for ( i = 0; i < no_combinations; i++ )
	{
		strcpy ( query, "SELECT " );
		strcat ( query, table[table_no].attribute[attribute_no] );
		
		strcat ( query, " FROM " );
		strcat ( query, table[table_no].name );
		
		strcat ( query, " WHERE " );
		
		for ( j = 0; input[i][j] != '\0'; j ++ )
		{
			if ( j != 0 )
			{
				strcat ( query, " AND " );
			}
			
			strcat ( query, table[table_no].attribute[( (int) ( input[i][j] - 1 ) )] );
			strcat ( query, " = '" );
			strcat ( query, row[( (int) ( input[i][j] - 1 ) )] );
			strcat ( query, "'" );
		}
		strcat ( query, ";" );
		
		fprintf ( fp, "%s\n", query );
	}
	
	return true;
}
