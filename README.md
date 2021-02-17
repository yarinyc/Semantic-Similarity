
EC2 configuration:

    1) Instance type - "M4Xlarge"
    2) Number of instances used: 9
    3) Region - "US_EAST_1"

EMR configuration:
    Hadoop version - 3.2.1
    EMR version - emr-6.2.0

**How to run the project:**
    
    1) Add to the config file:
        - your bucketName (bucket should already exist)
        - EC2 keypair
        - S3 paths to the input as a comma seperated list
    2) Run mvn clean install
    3) Upload all jars (except for localApplication.jar) to the s3 URI: s3://${YOUR_BUCKET_NAME}/jars/
    4) In the terminal run: java -jar localApplication.jar: this will run the map reduce flow
    5) To view the mapReduce process, refer to the aws EMR console 
    6) Final map reduce result files will be saved to the s3 URI: s3://**YOUR_BUCKET_NAME**/step_8_results/ in text format
    7) In the terminal run: java -jar localApplication -WEKA -> this will run the classifier code

**Communication, Running Times And Results:**

**1 file as input:**

    Total runtime: 8 minutes
    Weka output:
        Correctly Classified Instances           13294               92.9845 %
        Incorrectly Classified Instances         1003                7.0155 %
        Kappa statistic                          0.3737
        Mean absolute error                      0.1269
        Root mean squared error                  0.245
        Relative absolute error                  76.2613 %
        Root relative squared error              84.9512 %
        Total Number of Instances                14297
        
        === Confusion Matrix ===
         a     b     actual class
        12962    26 |     a = False
        977     332 |     b = True
        
        Accuracy: 92.9845422116528
        Precision: 0.9273743016759777
        Recall: 0.25362872421695953
        F1 score: 0.3983203359328135

    map-reduce:
    step1:
        Map output records=15959876
        Map output bytes=1288709999
        Combine input records=0
        Combine output records=0
    step2:
        Map output records=37068550
        Map output bytes=809631148
        Combine input records=37068550
        Combine output records=6349356
    step3:
        Map output records=4066765
        Map output bytes=76553601
        Combine input records=4066765
        Combine output records=1854329
    step4:
        Map output records=53028414
        Map output bytes=661129484
        Combine input records=53028414
        Combine output records=1098362
    step5:
        Map output records=4433911
        Map output bytes=160686739
        Combine input records=0
        Combine output records=0
    step6:
        Map output records=4704074
        Map output bytes=194638449
        Combine input records=0
        Combine output records=0
    step7:
        Map output records=577889
        Map output bytes=51359905
        Combine input records=577889
        Combine output records=69821
    step8:
        Map output records=26375
        Map output bytes=222182871
        Combine input records=0
        Combine output records=0

**Our implementation:**

In order to calculate the similarities between lexemes, we run a mapReduce job flow with the following steps:

*map-reduce design:*

    step 1: (pre-processing) takes the data set as input
        - convert all biarcs (lines of the input) into Biarc objects & convert each word in the Biarc into its lexeme.
          (using the stemmer tool from the assignment page)
        map: 	key=lineId, value=string biarc
                emit key=string root-lexeme, value: Biarc 
        reduce: key=string root-lexeme, value=list<Biarc>
                emit key=root-lexeme, value=Biarc (emit for each Biarc in the list) 
    
    step 2: takes the output of step 1 as input
        - for each biarc and for each lexeme in it, emit all its features (this gets us count(F=f,L=l))
        map:	key=lexeme, value: Biarc
                emit key=<lexeme,Feature> value=count (for each lexeme in the biarc and for each feature)
        reduce: key=<lexeme,Feature> value=list<count>
                * sum += all counts in list 
                emit key=<lexeme,Feature> value=sum (sum is count(F=f,L=l))
    
    step 3: takes the output of step 2 as input
        - calculate count(F=f) for each feature (count(F=f) = Σcount(F=f, L=l) for F=f )
        - calcuate count(F)
        map:	key=<lexeme,Feature>, value: count(F=f,L=l)
                emit key=Feature, value=count(F=f,L=l)
        reduce: key=Feature, value=list<count(F=f,L=l)>
                * sum += counts in list
                emit key=Feature value=sum (count(F=f))
                * hadoop counter: increase count(F) by sum
    
    step 4: takes the output of step 1 as input
        - calculate count(L=l) for each lexeme i.e. number of occurrences of each lexeme
        - calculate count(L)
        map:	key=lexeme, value=Biarc
                emit key=lexeme, value=Biarc.count
        reduce: key=lexeme, value=list<Biarc.count>
                * sum+= counts in list
                emit key=lexeme value=sum (count(L=l))
                * hadoop counter: increase count(L) by sum (number of all words)
    
    2 way join - join outputs of steps 2,3,4:
    
    step 5:
        - join outputs of steps 2,4
        map:	key=<lexeme,feature>, value=count(F=f,L=l)
                key=lexeme, value=count(L=l)
                emit key=<lexeme,feature> value=<"LF",count(F=f,L=l)>
                emit key=lexeme 		  value=<"L",count(L=l)>
        reduce: key=lexeme, value=list<count of some type>
                * we make sure count(L=l) arrives first
                for each count(F=f,L=l):
                emit key = <lexeme,feature>, value=<count(F=f,L=l),count(L=l)>
    
    step 6:
        - join outputs of steps 3,5
        - calculate all the assoc formulas
        setup:  take count(F) and count(L) (from s3)
        map:	key=<lexeme,feature>, value=<count(F=f,L=l),count(L=l)>
                key=feature, value=count(F=f)
                emit key=<lexeme,feature> value=<"LF",count(F=f,L=l),count(L=l)>
                emit key=lexeme 		  value=<"F",count(F=f)>
        reduce: key=Feature, value=list<count of some type>
                * we make sure count(F=f) arrives first
                for each <count(F=f,L=l),count(L=l),count(F=f)> do:
                * calculate all assoc formulas (we already have all counts now)
                * emit key=<l,f> value=[assoc_freq, assoc_prob, assoc_PMI, assoc_t-test]
    
    step 7:
        - for each lexeme from the golden standard build 4 assoc vectors
        setup:  load a set of all lexemes from the golden standard
        map:	key=<l,f> value=[list of assoc values]
                emit key=l value=<f,[list of assoc values]>
        reduce: key=l, value=list<f,[list of assoc values]>
                * (assume that we can hold all 4 vectors in memory)
                emit key=l, value=list<f,[list of assoc values]> (in next step this will be parsed into our vectors)
    
    step 8:
        - for each pair in golden standard build 24 coordinate vector
        setup:  load all pairs from the golden standard
        map:	key=l, value=[list of vectors(4 vectors)]
                for each pair in GS (<l,l'> or <l',l>) that includes l:
                emit key= <l,l'> or <l',l> respectively (only 1 pair), value = <tag-'arrived from l',[list of vectors(4 vectors)]>
        reduce: key=<l1,l2> value=list< <tag-'arrived from ...', [list of vectors(4 vectors)]> > (value is a list of size 2 always)
                * calculate for each type of similarity formula its value: sim_js, sim_dice etc. (we will have 6 different values per assoc type, 24 in total)
                * in total we will have 24 different values. (coordinate PMI-JS=val1, PMI-dice=val2, etc.)
                emit key=<l1,l2>, value=GS_vector of size 24

*Weka Classifier*

    - Take our data from the final result directory in our S3 bucket and download it to resources/rawData directory.
    - Concatenate all files to 1 input file: vectors.txt
    - convert our raw data file to csv format after some preprocessing: replacing all NaN/Infinity values with mean of feature columns
    - load data into instances object
    - create our classifier object: we used a randomForest model with max depth of 20
    - run 10 fold cross validation
    - displat final results: confusion matrix, accuracy, precision, recall and F1 score

