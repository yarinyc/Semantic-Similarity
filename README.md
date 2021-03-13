*Overview:*

	Using Hadoop map reduce we go over google's Biarc dataset and analyze the data according to this article: https://bib.irb.hr/datoteka/507889.ljubesic08-comparing.pdf
	We build Cooccurrence vectors that will be based on the words which are directly connected to the given word in the syntactic trees.
	For example, given the sentence "My dog also likes eating sausage" ,our syntactic-based method, will pick the feature 'dog' for 'likes',
	since there is a 'subj' edge between them. 
	We define the features as pairs of words and dependency-labels, i.e., the words which are connected in the syntactic tree to the given word and the label on the edges between them.
	After analyzing the data we use the Weka java API to train a model.
	We evaluate the trained model by applying 10-fold cross validation on the provided dataset.
	The final result are the Precision, Recall and F1 scores of the classifier.


EC2 configuration:

    1) Instance type - "M4Xlarge"
    2) Number of instances used: 8
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
    - create our classifier object: we used a randomForest model with max depth of 20 (Note: other tested models were logistic regression classifier,decision tree, multilayer perceptron)
    - run 10 fold cross validation
    - display final results: confusion matrix, accuracy, precision, recall and F1 score

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

**All files as input:**

    Total runtime: 37 minutes
    Weka output:

        Correctly Classified Instances           13500               94.0832 %
        Incorrectly Classified Instances         849                 5.9168 %
        Kappa statistic                          0.5124
        Mean absolute error                      0.1055
        Root mean squared error                  0.2195
        Relative absolute error                  63.3699 %
        Root relative squared error              76.094  %
        Total Number of Instances                14349
        
        === Confusion Matrix ===
        
            a     b     actual class
        13006    28 |     a = False
          821   494 |     b = True
        
        Accuracy: 94.08321137361489
        Precision: 0.946360153256705
        Recall: 0.37566539923954373
        F1 score: 0.537833424060969


    map-reduce:
    step1:
        Map output records=266930246
		Map output bytes=25337979955
		Combine input records=0
		Combine output records=0
    step2:
        Map output records=668842937
		Map output bytes=15989958533
		Combine input records=668842937
		Combine output records=100284478
    step3:
        Map output records=30733406
		Map output bytes=602860853
		Combine input records=30733406
		Combine output records=8502799
    step4:
		Map output records=935773268
		Map output bytes=12563640439
		Combine input records=935773268
		Combine output records=12773488
    step5:
		Map output records=31901939
		Map output bytes=1315123907
		Combine input records=0
		Combine output records=0
    step6:
		Map output records=33174222
		Map output bytes=1577878063
		Combine input records=0
		Combine output records=0
    step7:
		Map output records=6030533
		Map output bytes=544632990
		Combine input records=6030533
		Combine output records=137997
    step8:
		Map output records=28469
		Map output bytes=2728951210
		Combine input records=0
		Combine output records=0

**Analysis:**

    Below are wordPair examples of TP, FP, TN, FN predictions made by our model:
    
    TP:
      1) butterfly, animal
         For obvious reasons, there is a connecntion between both words and the model identifies it correctly
      2) toaster, device
         Same explanation as above
      3) lion, predator
         Same explanation as above

    FP:
      1) glider, animation
         It seems that some contexts of both words can share common features:
         flight-conj, flew-rcmod, dawn-nn, imagin-rcmod, etc.
      2) bomb, limitation
         It seems that some contexts of both words can share common features:
         militari-amod, concern-partmod, object-dobj, secret-amod, etc.
      3) mug, co-author
         It seems that some contexts of both words can share common features:
         and-amod, for-dep, object-dobj, write-rcmod, etc.

    TN:
      1) piano, airspace
         Both words are semanticaly unrelated, and the model identifies that
      2) broccoli, bomb
         Same explanation as above
      3) banana, strategy
         Same explanation as above

    FN:
      1) bull, mammal
         It seems that the size of the intersection of feature sets of both words is of size 384
         and the size of the symmetric difference is 7319.
         Therefore it's easy to see how the model classifies the words as not related.
      2) bottle, container
         It seems that the size of the intersection of feature sets of both words is of size 1328
         and the size of the symmetric difference is 13631.
         Therefore it's easy to see how the model classifies the words as not related.
      3) cannon, weapon
         It seems that the size of the intersection of feature sets of both words is of size 259
         and the size of the symmetric difference is 2553.
         Therefore it's easy to see how the model classifies the words as not related.
