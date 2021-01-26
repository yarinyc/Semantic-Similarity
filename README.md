
EC2 configurations we used:
1) Instance type - "m4.large"
2) Number of instances used: 8
3) Region - "US_EAST_1"

EMR configuration:

Hadoop version - 3.2.1
EMR version - emr-6.2.0

**How to run the project:**
 1) Add your bucketName (bucket should already exist) and ec2 keypair to the config file (first row - bucket name, second row - keypair) 
 2) Run mvn clean install
 3) Upload all jars (except for localApplication.jar) to the s3 URI: s3://**YOUR_BUCKET_NAME**/jars/
 4) In the terminal run: java -jar localApplication.jar
 5) To view the mapReduce process, refer to the aws EMR console 
 6) Final result files will be saved to the s3 URI: s3://**YOUR_BUCKET_NAME**/step_6_sort_results/ in text format

**Running times and statistics:**

1) With local aggregation: 15 minutes (not including provisioning and bootstrapping times)
* step1:
  * Combine input records=71119513
  * Combine output records=1686120
* step2:
  * Combine input records=3370222
  * Combine output records=35342
* step3:
  * Combine input records=3370222
  * Combine output records=35342
* step4: (No combiner used)
  * Map output records=13740
  * Map output bytes=219379
* step5: (No combiner used)
  * Map output records=1689486
  * Map output bytes=65003588
* step6: (No combiner used)
  * Map output records=1685111
  * Map output size in bytes=67672720

2) Without local aggregation: 16 minutes (not including provisioning and bootstrapping times)
* step1:
  * Map output records=71119513
  * Map output size in bytes=3822592448
* step2:
  * Map output records=3370222
  * Map output bytes=87625772
* step3:
  * Map output records=3370222
  * Map output size in bytes=87625772
* step4:
  * Map output records=1698881
  * Map output size in bytes=69577662
* step5:
  * Map output records=1685111
  * Map output size in bytes=67672720
 
**Link to output files**
* s3://s3bucket-d797b2f9e96963c1/step_6_sort_results/
  
**Analysis:**

1) "אביו של"
 * אביו של רבינו	2.252608E-6
 * אביו של אחד	1.4695687E-6
 * אביו של דוד	1.2532015E-6
 * אביו של אברהם	1.1921054E-6
 * אביו של נתן	7.5672125E-7
  
* This output seems to reasonable since most of the 3rd words are biblical names, or a commonly used adjective in the bible or other religious texts.

2) "כוחו של"
  * כוחו של ה	1.1428484E-6
  * כוחו של עם	8.4629374E-7
  * כוחו של היצר	6.6688165E-7
  * כוחו של יעקב	5.3501515E-7
  * כוחו של צבא	4.9962824E-7

* This output seems to reasonable since most of the 3rd words are subjects which may hold power.* 

3) "להסביר את"
  * להסביר את כל	2.947639E-6
  * להסביר את התפתחות	2.7571562E-7
  * להסביר את דעת	2.7104838E-7
  * להסביר את ריבוי	2.605502E-7
  * להסביר את סדר	2.605502E-7

* This output seems to reasonable since most of the 3rd words are some sort of phenomenon (in the first trigram the 3rd word is a more general word).

4) "אינני יודע"
  * אינני יודע מתי	1.4770577E-6
  * אינני יודע במה	1.0726723E-6
  * אינני יודע איזו	5.2517015E-7
  * אינני יודע בעצמי	4.79007E-7
  * אינני יודע גם	4.029334E-7

* We are not sure if this list is reasonable or not.

5) "אינני יכול"
 * אינני יכול להסביר	5.409554E-7
 * אינני יכול לקבוע	4.4574085E-7
 * אינני יכול לעזור	4.418366E-7
 * אינני יכול לחשוב	3.9902832E-7
 * אינני יכול לשבת	3.0367312E-7

* This output seems to reasonable since most of the 3rd words are verbs, and the ones that are most commonly used have higher probability.

6) "חייב אדם"
 * חייב אדם ללמד	2.0026935E-6
 * חייב אדם לדעת	6.2682767E-7
 * חייב אדם לקיים	2.9616513E-7
 * חייב אדם להכיר	2.1966498E-7
 * חייב אדם לשלוח	1.6650428E-7

* We are not sure if this list is reasonable or not.

7) "יש לו"
 * יש לו זכות	1.0836155E-5
 * יש לו רשות	9.491315E-6
 * יש לו מקום	6.7657706E-6
 * יש לו רק	6.4610567E-6
 * יש לו את	4.8392526E-6

* We are not sure if this list is reasonable or not.

8) "לברר את"
 * לברר את הדבר	3.5419957E-6
 * לברר את ענין	3.9902832E-7
 * לברר את אמיתות	3.7852854E-7
 * לברר את מהותו	2.7104838E-7
 * לברר את זהותם	2.4325536E-7

 * This output seems to reasonable since most of the 3rd words are things we amy need to investigate about.

9) "לשמור על"
 * לשמור על קדושת	2.133619E-6
 * לשמור על כך	1.6570267E-6
 * לשמור על המסורת	1.6177061E-6
 * לשמור על גבולות	1.0286997E-6
 * לשמור על עצמאותם	9.387844E-7

* We are not sure if this list is reasonable or not.

10) "יצר הרע"
  * יצר הרע של	2.7208748E-6
  * יצר הרע מצוי	6.1692356E-7
  * יצר הרע מן	5.773846E-7
  * יצר הרע הם	1.7866411E-7
  * יצר הרע לאדם	1.7866411E-7

* We are not sure if this list is reasonable or not.

**Our implementation:**

In order to calculate the probability of each trigram in the dataset, we run a mapReduce job flow with the following steps:
  1) countTrigrams:
     * Combiner class was used because the summing operation is commutative
     * The Mapper reads the input data set and does the following:
       * filters out trigrams that contain illegal words (does not emit this trigram to the reducer).
       * for each legal trigram randomly partitions its occurrences to 2 halves.
       * emits: key=trigram , value =<count for half 0, count for half 1>.
       * adds the number of occurrences to a global counter: N (saved in hadoop context). 
     * The Reducer reads the output of the Mapper and does the following:
       * aggregates the counts of each trigram and emits: key=trigram , value=<sum of counts for half 0, sum of counts for half 1>
       * saves the N counter to s3 for later use.
       
  2) computeNr:
     * Combiner class was used because the summing operation is commutative
     * The Mapper reads the output of step 1 and does the following:
       * emits key=<group,r> , value=1 for each group (0 or 1).
         Each trigram is emitted for each half it appears in (value is 1 because it appeared in group0/group1 r times).
     * The Reducer reads the output of the Mapper and does the following:
       * emits key=<group,r> , value=sum of 1's.
         At the end this means that N_group_r appeared 'value' times.
         
  3) computeTr:
     * Combiner class was used because the summing operation is commutative
     * The Mapper reads the output of step 1 and does the following:
       * emits key=<group0, r0> or <group1, r1> , value= r1 or r0 respectively.
         In order to calculate T01_r for example, we need to sum all the trigrams from group1 which appeared r times in group0.
     * The Reducer reads the output of the Mapper and does the following:
       * emits key=<group0/group1 ,r0/r1> , value=sum of r1/r0's.
         At the end this means that T_group_r appeared 'value' times.
	   
  4) computeProbability:
	 * The Mapper reads the outputs of step 2 step 3 and does the following:
	   * emits key=r , value = T_group_r/N_group_r.
	   In order to calculate P for each r, we need to first send all T and N values of each r to the same reducer 
	 * The Reducer reads the output of the Mapper and does the following:
	   * emits key=<r,1> , value= probability
	     At the end this means this means that the probability according to the formula for a specific r is value, and the key (<r,1>) is used later for the join step.
	     If the denominator or numerator are equal to 0, we return NaN as the probability.
       
  5) joinAndComputeProbability:
	 * We use a groupingComparator and a sortComparator to make sure P and Trigrams are send to the same reducer, and that P arrives first out of all K-V pairs
     * The Mapper reads the output of all previous steps and joins the 3 outputs for each r value (r is the foreign key):
	   * emits key=<r, 1\2>  , value = probability\trigram. 
       The values emitted extend the abstract class Joinable, and thus can be sent to the reducer as values.
       The reducer decides if the joinable object is a probability or trigram according to the Joinable getType() method.
     * The Reducer reads the output of the Mapper and does the following:
       * emits key=trigram , value=probability
       For each trigram emits the probability according to the given formula.
	   
  6) sortOutput: 
     * The Mapper reads the output of step 4 and does the following:
       * emits key=input key , value=input value
       * we use hadoop's sort by key mechanism to send all data already sorted according to our compareTo() method of the key (trigram).
         The compareTo method sorts by the first 2 words and then by the probability value.
     * The Reducer reads the output of the Mapper and emits the <key,value> pair without any change (only now they are sorted).
       
