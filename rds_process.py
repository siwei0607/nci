#!/usr/bin/env python
import sys,os, time
from subprocess import Popen, PIPE, STDOUT

#repo: the name of dataset
#latest: the date of last harvest records

def harvest_pro(repo):
        command = 'sudo java -jar /home/ubuntu/RD-Switchboard/Production/Harvesters/harvest_%s/harvester_oai-1.3.6.jar /home/ubuntu/RD-Swit$
        os.popen("sudo -S %s"%(command), 'w').write('Sw8867612')
        time.sleep(5)
        print "%s is harvested!"%repo

def import_pro(repo):
        l = open('/home/ubuntu/RD-Switchboard/Production/Harvested-XML/%s/nci/rif/latest.txt'%repo.lower(), 'r+')
        latest = l.readline()
        print "The current data is havested on "+str(latest)
        time.sleep(3)

        f = open('/home/ubuntu/RD-Switchboard/Production/Importers/Import_ANDS_%s/properties/import_ands.properties'%repo, 'r+')
        outfile = '/home/ubuntu/RD-Switchboard/Production/Importers/Import_ANDS_%s/properties/import_ands_latest.properties'%repo
        with open(outfile, 'w')as f1:
                for row in f.readlines():
                        if row[:11]=='ands.source':
                                f1.write('ands.source='+repo.lower())

                        else:
                                if row[:9]=='ands.xml=':
                                        f1.write('ands.xml=/home/ubuntu/RD-Switchboard/Production/Harvested-XML/%s/nci/rif/%s/datasets'%(re$
                                else:
                                        f1.write(str(row))

        time.sleep(5)

        cmd1 = 'cd /home/ubuntu/RD-Switchboard/Production/Neo4j'
        cmd2 = 'sudo ./bin/neo4j stop'
        final = Popen("{}; {}".format(cmd1, cmd2), shell=True, stdin=PIPE,
                stdout=PIPE, stderr=STDOUT, close_fds=True)
        stdout, nothing = final.communicate()
        log = open('log', 'w')
        log.write(stdout)
        log.close()
        time.sleep(10)
        print "Neo4j is stopping, please wait..."

        cmd3 = 'sudo java -jar /home/ubuntu/RD-Switchboard/Production/Importers/Import_ANDS_%s/import_ands-1.4.0.jar /home/ubuntu/RD-Switch$
        os.popen("sudo -S %s"%(cmd3), 'w').write('Sw8867612')
        time.sleep(3)
        print "%s is imported"%repo

        cmd1 = 'cd /home/ubuntu/RD-Switchboard/Production/Neo4j'
        cmd2 = 'sudo ./bin/neo4j start'
        final = Popen("{}; {}".format(cmd1, cmd2), shell=True, stdin=PIPE,
                stdout=PIPE, stderr=STDOUT, close_fds=True)
        stdout, nothing = final.communicate()
        print "Neo4j is starting, please wait..."
        time.sleep(10)
        print "Done!"

def main():
  harvest_pro('geonetworkrs0')
  import_pro('Geonetworkrs0')

if __name__ == '__main__':
    main()