# 2015-08-26

gpc all
gpc make target1 target2 ...
gpc comment "my comment"
gpc make target0
gpc comment "den här datafilen skapade den fina figuren"
gpc status

gpc explain ~/figs/en_fin_figur_på_min_disk.png
gpc purge??
    för att radera cache-filer, t ex äldre än ett visst datum

gpc make --commit ae723434 target1
cp target1 target1_new
gpc make --commit b9334.. target1
cmp target1 target1_new

    om den finns: leverera ut
    om inte: säg nej, du får stasha, checka ut och köra make igen


gpc ändrar inte i git, bara läser

ifneedbe:
    gpc init
        skapa .gpc/
        skapa .gpc/config

gpc cached target1 ~/temp
    dumpa alla "target1" i ~/temp, omdöpta med t ex make-tid som prefix

gpc make --reproduce target1
    om du inte har någon digest, kasta felmeddelande
    om du har gammal digest, kör igen och jämför med ny digest; rapportera eventuell skillnad
        om cachad fil finns, dumpa ut resultat från då och nu

    (detta bör för övrigt göras alltid när man kör gpc make och digest finns, men inte cachat resultat)

och innan man skriver sin artikel:
gpc all --reproduce


gpc tag label1 label2 label3
gpc tag -d label2
    inte så hög prioritet på den saken

gpc log


sumatra har sync för att merga log-filer
    - vi behöver eventuellt inte det?
    - men cache-katalogerna ska man bara kunna kopiera in

    Möjligen är ändå "gpc merge" önskvärt eftersom den kan kolla motsvarande gpc make --reproduce, dvs att eventuella skillnader i det som ska vara identiskt rapporteras

smt version
gpc --version

# 2015-09-03


    gpc make [output]:
        find out what is needed in directory, current state of system, etc
        from that, calculate hash/id of the requested output
        if not cached output exists:
            setup working directory
            start separate process to run the task that directory
            wait
            save record
            cache output
            clean/remove working directory
            
        when output exists:
            present it to user



# 2015-09-04

    
    - File id (fid): hash of contents
    - Target id (tgtid): relpath and fid
    - Task id (tskid): hash of procedure, significant context, etc
    - Execution id (eid): tskid, [tgtid for each target]

## Get tgtid:

* Get T, the Task that produces the Target
* Get all Targets the Task T depends on
* For all inputs, get tgtid
* Compute eid for T
* Check in execution database for eid:
    * If it exists, we can get the tgtid of all outputs and return the requested one.
    * Else, the execution has never before been done.
        * Return tgtid of requested target.

## Execute:
* Prepare execution folder.
* Execute.
* Compute fids of newly produced files and save files under their fids.
* Save a record in the execution database: eid --> tgtids
* Save files under their fids.



## Get target:

* get tgtid
* from target database, get fid and path
* check in file storage if file is there
    - if not, execute
* get file from storage and put it at path
