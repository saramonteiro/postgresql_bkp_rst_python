# Module to use pg_dump and pg_restore cmds 
import subprocess
# Module to parser parameters 
import getopt, sys
import os

def backup(host, port, database, user, verbose):
    try:
        subprocess_params = [
            'pg_dump',
             '--host={}'.format(host),
             '--username={}'.format(user),
             '--dbname={}'.format(database),
             '--port={}'.format(port),
             '-Fc',
             '-w',
             '-f{}'.format(os.environ['DUMPFILE']),
        ]
        if verbose:
            subprocess_params.append('-v')
        process = subprocess.Popen(subprocess_params, stdout=subprocess.PIPE)
        output = process.communicate()[0]
        if int(process.returncode) != 0:
            print('Command failed. Return code : {}'.format(process.returncode))
            exit(1)
        else:
            print("Backup has completed.")
        return output
    except Exception as e:
        print("Issue with the db backup : {}".format(e))
        exit(1)

def dropdb(host, port, database, user, verbose):
    try:
        subprocess_params = [
            'dropdb',
             '--host={}'.format(host),
             '--username={}'.format(user),
             '--port={}'.format(port),
             '-w',
        ]
        if verbose:
            subprocess_params.append('-e')
        subprocess_params.append(database)
        process = subprocess.Popen(subprocess_params, stdout=subprocess.PIPE)
        output = process.communicate()[0]
        if int(process.returncode) != 0:
            print('Command failed. Return code : {}'.format(process.returncode))
        else:
            print("Database was dropped.")
        return output
    except Exception as e:
        print("Issue with the dropdb : {}".format(e))

def createdb(host, port, database, user, verbose):
    try:
        subprocess_params = [
            'createdb',
             '--host={}'.format(host),
             '--username={}'.format(user),
             '--port={}'.format(port),
             '-w',
        ]
        if verbose:
            subprocess_params.append('-e')
        subprocess_params.append(database)
        process = subprocess.Popen(subprocess_params, stdout=subprocess.PIPE)
        output = process.communicate()[0]
        if int(process.returncode) != 0:
            print('Command failed. Return code : {}'.format(process.returncode))
        else:
            print("Database was recreated.")
        return output
    except Exception as e:
        print("Issue with the createdb : {}".format(e))   

def restore(host, port, database, user, verbose):
    try:
        subprocess_params = [
            'pg_restore',
            '--no-owner',
             '--host={}'.format(host),
             '--username={}'.format(user),
             '--dbname={}'.format(database),
             '--port={}'.format(port),
             '-Fc',
             '-w',
             '-j 4 '
        ]
        if verbose:
            subprocess_params.append('-v')
        subprocess_params.append(os.environ['DUMPFILE'])
        process = subprocess.Popen(subprocess_params, stdout=subprocess.PIPE)
        output = process.communicate()[0]
        if int(process.returncode) != 0:
            print('Command failed. Return code : {}'.format(process.returncode))
            exit(1)
        else:
            print("Restore has completed.")
        return output
    except Exception as e:
        print("Issue with the db restore : {}".format(e))
        exit(1)

def usage():
    print("usage: python3 updatedb.py -b <hostname>:<port>:<db>:<user> -r <hostname>:<port>:<db>:<user>")
    print("-h : Help")
    print("-d : Drop and Recreate database")

# Possible flows includes:
# - Only Backup
# - Only Restore
# - Backup and Restore
# - Backup, drop/recreate and Restore
# - Drop/recreate and Restore
def process_flow(backup_args, drop, restore_args, verbose):
    print("Initiating process. It may take some time. Activate verbose mode to follow outputs.")
    if backup_args:
        os.environ['PGPASSFILE'] = os.environ['PGPASS_BKP_PATH']
        if verbose:
            print("PGPASS file used for backup:")
            print(os.environ['PGPASSFILE'])
        backup(backup_args[0], backup_args[1], backup_args[2], backup_args[3], verbose)
    if restore_args:
        if drop:
            os.environ['PGPASSFILE'] = os.environ['PGPASS_POSTGRES_PATH']
            if verbose:
                print("PGPASS file used for Droping and Recreating the database to be restored:")
                print(os.environ['PGPASSFILE'])
            dropdb(restore_args[0], restore_args[1], restore_args[2], restore_args[3], verbose)
            createdb(restore_args[0], restore_args[1], restore_args[2], restore_args[3], verbose)
        os.environ['PGPASSFILE'] = os.environ['PGPASS_RST_PATH']
        if verbose:
            print("PGPASS file used for Restore:")
            print(os.environ['PGPASSFILE'])
        restore(restore_args[0], restore_args[1], restore_args[2], restore_args[3], verbose)

def main():
    backup_args=[]
    restore_args=[]
    drop = False
    verbose = False

    if os.getenv('DUMPFILE') == None:
        os.environ['DUMPFILE']= 'mybackup.dump'
 
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hvdb:r:", ["help"])
    except getopt.GetoptError as err:
        # print help information and exit:
        print(err)
        usage()
        sys.exit(2)

    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-b"): #backup
            backup_args = list(a.split(":"))
            if len(backup_args) != 4:
                print("Missing arguments")
                usage()
                sys.exit()
        elif o in ("-r"): # restore
            restore_args = list(a.split(":"))
            if len(restore_args) != 4:
                print("Missing arguments")
                usage()
                sys.exit()
        elif o in ("-d"): # drop
            drop = True
        elif o in ("-v"): # drop
            verbose = True
        else:
            assert False, "unhandled option"    
    # Allows user to enter options in any order
    process_flow(backup_args, drop, restore_args, verbose)

if __name__ == "__main__":
    main()
