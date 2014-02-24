T2 Big Data Hackathon Soruları
=================

Yapılması gerekenler
-----------

**1.** Projeyi Git ([http://git-scm.com/)](http://git-scm.com/)) kullanarak `git clone https://github.com/serkan-ozal/t2-hackathon-answers.git` komutu ile yada proje sayfasında sağ alt tarafta bulunan **Download Zip** ile çekebilirsiniz. 

**2.** `pom.xml` dosyasında bulunan 

~~~
<aws.accessKey>[Your AWS Access Key]</aws.accessKey>
<aws.secretKey>[Your AWS Secret Key]</aws.secretKey>
<aws.accountNo>[Your AWS Account No]</aws.accountNo>
~~~

kısımında kendi hesabınıza ait değerleri girmelisiniz. [https://portal.aws.amazon.com/gp/aws/securityCredentials?)](https://portal.aws.amazon.com/gp/aws/securityCredentials?) adresinde bulunan **Access Credentials** bölümünde bulunan **Create a new Access Key** ile kendinize ait credentials yaratabilirsiniz. Bu credentials dosyasının içinde size ait AWS **Access Key** ve **Secret Key** bulunmaktadır.

**3.** Projeyi derlemek, build etmek ve deploy etmek için Maven ([http://maven.apache.org/](http://maven.apache.org/)) gereklidir. Maven'ın nasıl kullanılacaği ile ilgili bilgiyi [http://maven.apache.org/guides/getting-started/maven-in-five-minutes.html](http://maven.apache.org/guides/getting-started/maven-in-five-minutes.html) adresinden bulabilirsiniz.

Projeyi build etmek için gerekli komutlar proje dizinindeki `build.sh` dosyasında, deploy etmek için gerekli komutlar da `deploy.sh` dosyasında bulunmaktadır.

Deploy sonucu size ait olan Map/Reduce job jar dosyası `t2-hackathon-answers-[Your AWS Account No]` dizininde oluşacaktır. Bu dizini deploy yapmadan önce oluşturmanız gerekmektedir. Bu dizini [https://console.aws.amazon.com/s3](https://console.aws.amazon.com/s3) adresinden **S3** üzerinde **Create Bucket** diyerek oluşturabilirsiniz.

**4.** Map/Reduce job'ını nasıl başlatacağınız ile ilgili gerekli bilgiyi [http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-launch-custom-jar-cli.html](http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-launch-custom-jar-cli.html) adresinden bulabilirsiniz.

**5.** Çözümler Hadoop 2 - YARN ile yapıldığı için Map/Reduce job jar'ını AWS üstünde **AMI version 3.0.3** ve **Hadoop version 2.2.0** ile çalıştırmanız gerekmektedir.

Açıklamalar
-----------

**[1].** Tüm Map/Reduce uygulamaları girdi verisi dizinini ve çıktı verisi dizinini programlarına argüman olarak alacaktır.

**[2].** 1. parametre olan girdi verisi dizini işlenecek tweet verilerini tutan dizindir ve burada içinde tweet verisi tutan binlerce dosya bulunmaktadır. Uygulamalarda bu dizinde bulunan tüm dosyalardaki veriler incelenecektir. 

**[3].** Yaklaşık **100'er GB** lık gerçek girdi verisi `s3n://t2-hackathon-question-data/<soru_no>` dizinlerinde bulunmaktadır. Örneğin 1. sorunun veri seti `s3n://t2-hackathon-question-data/1` dizinindeki tüm dosyalardır.

**[4].** Yaklaşık **300 MB** lık örnek girdi verisi `s3n://t2-hackathon-question-sampledata/<soru_no>` dizinlerinde bulunmaktadır. Örneğin 1. sorunun örnek veri seti `s3n://t2-hackathon-question-sampledata/1` dizinindeki tüm dosyalardır.

**[5].** 2. parametre olan çıktı verisi dizinini çalışan Map/Reduce uygulamasının sonucunun yazıldığı dizindir.

**[6].** Çözdüğünüz soruların cevaplarını e-mail adreslerinize mail olarak gönderilen kullanıcı adı ve şifre ile [https://t2-bigdata-hackathon.signin.aws.amazon.com/console](https://t2-bigdata-hackathon.signin.aws.amazon.com/console) adresinden giriş yaparak kendi dizininize yüklemeniz gerekmektedir. AWS bilgilerini gönderip hala mail alamayan arkadaşlar lütfen benim ile (serkan.ozal@t2.com.tr) iletişime geçsinler. 

Bu dizinler code-challenge kısmına katılan arkadaşların cevaplarını ve kaynak kodlarını upload edecekleri dizinler. Değerlerndirmeyi yaparken bizler bu dizinleri kullanacağız. Herkes sadece kendi dizinlerine girebilir. 

Herkesin kendi dizinlerinin altında **answers** ve **src** diye alt dizinler var. Sonuçlarınızı **answers** dizini altına soru numarası ile, o soru için çözümünüzü (kaynak kod, script, vs) ise **src** dizini altına soru numarası ile upload etmeniz gerekecek.

**[7].** Her yarışmacının çıktı verisi `s3n://t2-bigdata-hackathon-<account_no>` dizininde bulunacaktır. Bu dizinler yarışma öncesi bizim tarafımızdan yaratılmıştır. Buradaki **account_no** değeri size özeldir ve sadece siz ve bizler tarafından erişilebilmektedir. Tüm sonuç dosyaları size özel çıktı verisi dizini altında `answers/<soru_no>/output.txt` dosyasında bulunacaktır. Örneğin sizin hesap numaranız **1234-5678-9000** olsun. Bu durumda sizin 1. soru için olan cevabınız `s3n://t2-bigdata-hackathon-1234-5678-9000/answers/1/output.txt` dosyasında bulunmalıdır.

**[8].** Soruların örnek girdi verisi üstündeki sonuçları `s3n://t2-hackathon-sampleanswers` altında bulunmaktadır. Örneğin 1. sorunun örnek veri seti üstündeki sonucu `s3n://t2-hackathon-sampleanswers/1/output.txt` dosyasında bulunmaktadır ve [https://s3.amazonaws.com/t2-hackathon-sampleanswers/1/output.txt](https://s3.amazonaws.com/t2-hackathon-sampleanswers/1/output.txt) linki ile ulaşılabilmektedir. Uygulamalarınızı önce örnek veri seti üstünde çalıştırıp sonuçlarınızı bu örnek veri setinin cevapları ile karşılaştırmanız önerilir.

**[9].** JSON formatındaki tweet verilerini nesne yapısına dönüştürmek için,

   **[9.1].** Maven dependency olarak kullanmak isteyenler kendi `pom.xml`'lerine aşağıdaki dependency'yi ekleyebilirler.
	
> ```xml
	<dependency>
		<groupId>org.twitter4j</groupId>
		<artifactId>twitter4j-core</artifactId>
		<version>3.0.3</version>
	</dependency>
	
   **[9.2].** External library olarak kullanmak isteyenler, [http://repo1.maven.org/maven2/org/twitter4j/twitter4j-core/3.0.3/twitter4j-core-3.0.3.jar](http://repo1.maven.org/maven2/org/twitter4j/twitter4j-core/3.0.3/twitter4j-core-3.0.3.jar) adresinden ilgili library'yi indirebilirler.

**[10].** JSON verisini gerekli nesne yapısına dönüştürmek için aşağıdaki kod parçacığını örnek olarak kullanabilirsiniz.

> ```java
	twitter4j.Status status = twitter4j.json.DataObjectFactory.createStatus(jsonString);
	
**[11].** Yarışmada her kullanıcıya **50$**'lık AWS kredisi sağlanacaktır. Soruları çözerken bu miktarın yeterli olması için [AWS EMR Spot instance](http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-plan-spot-instances.html) kullanmanız **kesinlikle** önerilir.

**[12].** Çözümlerinizi ([https://github.com/serkan-ozal/t2-hackathon-mapreduce](https://github.com/serkan-ozal/t2-hackathon-mapreduce)) örneğinde olduğu gibi Hadoop 2 - YARN ile yapacaksanız Map/Reduce job jar'ını AWS üstünde **AMI version 3.0.3** ve **Hadoop version 2.2.0** ile çalıştırmanız gerekmektedir. Fakat bu konfigürasyonlar ile job'i web console'dan en düşük **Medium** instance tipi ile çalıştırabilirsiniz. Bu instance tipi ile de 9 soru için AWS krediniz muhtemelen yetmeyecektir. Bu sebeple bunları **Small** instance tipi ile çalıştırmanız önerilir. Bu konfigürasyonlar ile job'un **Small** instance tipi ile çalıştırılmasına AWS web console'dan izin vermese de Rest API ve SDK API (Java, .NET, ...) ile izin vermektedir. Bu sebeple [http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-plan-spot-instances.html](http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-plan-spot-instances.html) dökümanı ve altındaki bağlantılı dökümanları incelemeniz kesinlikle önerilir.

**[13].** Soruların örnek veriler üstündeki cevaplarına `https://s3.amazonaws.com/t2-hackathon-sampleanswers/<soru_no>/output.txt` adresinden erişilebilir. Örneğin 1. sorunun örnek veriler üstündeki cevabına [https://s3.amazonaws.com/t2-hackathon-sampleanswers/1/output.txt](https://s3.amazonaws.com/t2-hackathon-sampleanswers/1/output.txt) adresinden erişilebilir.

**[14].** Soruların gerçek veriler üstündeki cevaplarına `https://s3.amazonaws.com/t2-hackathon-answers/<soru_no>/output.txt` adresinden erişilebilir. Örneğin 1. sorunun gerçek veriler üstündeki cevabına [https://s3.amazonaws.com/t2-hackathon-answers/1/output.txt](https://s3.amazonaws.com/t2-hackathon-answers/1/output.txt) adresinden erişilebilir.



Örnek veri
-----------

[https://s3.amazonaws.com/t2-bigdata-hackathon-sampledata/Sample_Data.txt](https://s3.amazonaws.com/t2-bigdata-hackathon-sampledata/Sample_Data.txt) linkinden yaklaşık 150 MB'lık örnek veriye ulaşabilirsiniz.

> ```json
{
	"filter_level":"medium",
	"contributors":null,
	"text":"@ItbCruz: Aç değilim ama acıkmak istiyorum yemek, yemek istiyorum",
	"geo":{
		"type":"Point",
		"coordinates":[
			41.01536547,
			29.23015032
		]
	},
	"retweeted":false,
	"in_reply_to_screen_name":null,
	"truncated":false,
	"lang":"tr",
	"entities":{
		"symbols":[
		],
		"urls":[
		],
		"hashtags":[
		],
		"user_mentions":[
			{
				"id":1367863476,
				"name":"ALI",
				"indices":[1,9],
				"screen_name":"ItbCruz",
				"id_str":"1367863476"
			}
		]
	},
	"in_reply_to_status_id_str":null,
	"id":427195214810595329,
	"source":"<a href=\"http://twitter.com/download/iphone\" rel=\"nofollow\">Twitter for iPhone<\/a>",
	"in_reply_to_user_id_str":null,
	"favorited":false,
	"in_reply_to_status_id":null,
	"retweet_count":0,
	"created_at":"Sat Jan 25 21:44:10 +0000 2014",
	"in_reply_to_user_id":null,
	"favorite_count":0,
	"id_str":"427195214810595329",
	"place":{
		"id":"682c5a667856ef42",
		"bounding_box":{
			"type":"Polygon",
			"coordinates":[
				[
					[
						25.663883,
						35.817497
					],
					[
						25.663883,
						42.109993
					],
					[
						44.822762,
						42.109993
					],
					[
						44.822762,
						35.817497
					]
				]
			]
		},
		"place_type":"country",
		"contained_within":[
		],
		"name":"Türkiye",
		"attributes":{
		},
		"country_code":"TR",
		"url":"https://api.twitter.com/1.1/geo/id/682c5a667856ef42.json",
		"country":"Türkiye",
		"full_name":"Türkiye"
	},
	"user":{
		"location":"Turkey Istanbul",
		"default_profile":false,
		"profile_background_tile":true,
		"statuses_count":9679,
		"lang":"tr",
		"profile_link_color":"E70009",
		"profile_banner_url":"https://pbs.twimg.com/profile_banners/180525314/1388595872",
		"id":180525314,
		"following":null,
		"protected":false,
		"favourites_count":267,
		"profile_text_color":"828282",
		"description":"Bir kişiyi zayıflıkları ile yargılamak okyanusun gücünü tek bir dalgayla ölçmeye benzer. King Elvis and Magic Chef\nMessageMe pin: DB812VJY",
		"verified":false,
		"contributors_enabled":false,
		"profile_sidebar_border_color":"000000",
		"name":"Doğukan Kurhan",
		"profile_background_color":"000000",
		"created_at":"Thu Aug 19 21:12:44 +0000 2010",
		"default_profile_image":false,
		"followers_count":613,
		"profile_image_url_https":"https://pbs.twimg.com/profile_images/378800000539828532/88d95142ca3c6a120ae29aed9cc1a468_normal.jpeg",
		"geo_enabled":true,
		"profile_background_image_url":"http://a0.twimg.com/profile_background_images/378800000090042106/9f2fc371083b37c2c1c63aaf6b88349d.jpeg",
		"profile_background_image_url_https":"https://si0.twimg.com/profile_background_images/378800000090042106/9f2fc371083b37c2c1c63aaf6b88349d.jpeg",
		"follow_request_sent":null,
		"url":"http://instagram.com/dogukankurhan",
		"utc_offset":-18000,
		"time_zone":"Quito",
		"notifications":null,
		"profile_use_background_image":true,
		"friends_count":232,
		"profile_sidebar_fill_color":"0F0F0F",
		"screen_name":"dogukankurhan",
		"id_str":"180525314",
		"profile_image_url":"http://pbs.twimg.com/profile_images/378800000539828532/88d95142ca3c6a120ae29aed9cc1a468_normal.jpeg",
		"listed_count":0,
		"is_translator":false
	},
	"coordinates":{
		"type":"Point",
		"coordinates":[
			29.23015032,
			41.01536547
		]
	}
}

Sorular
-----------

Soru-1 [10 Puan]
-----------
`s3n://t2-hackathon-question-data/1` dizini altında bulunan tüm tweetler incelenerek ülke kodları ve bunların kaç tane geçtiğini bulan bir Map/Reduce uygulamasını yazınız. 

**Açıklamalar:**

1. Ülke kodu alanı tweet datasındaki `place` alanının içindeki `country_code` değeridir. 
2. Ülke kodu değeri 2 karakterlidir ve `country_code` değeri boş olan tweetlerin `country_code` değeri `??` olarak kabul edilecektir. 
3. Çıktıda her satırda ülke kodu ve bu ülke kodunun kaç kere geçtiği bilgisi olacaktır. 
4. Çıktıdaki ülke kodu ve sıklık değeri arasında bir tane **boşluk** olacaktır. 
5. Satırlar ülke kodlarının string değerlerine göre **küçükten büyüğe** göre **artan** sırada olacaktır.
6. Örnek girdi verisi `s3n://t2-hackathon-question-sampledata/1` dizininde, çıktı ise `s3n://t2-hackathon-sampleanswers/1/output.txt` dosyasında mevcuttur.

**Örnek çıktı:**

~~~
?? 19682
...
GB 24643
...
TR 30881
...
~~~

Soru-2 [15 Puan]
-----------
`s3n://t2-hackathon-question-data/2` dizini altındaki tüm binary dosyalar **16** byte'lık sabit büyüklükte binary kayıtlar içermektedir. Bu kayıtlar **8 byte**'lık (long) **grup bilgisi** ve **8 byte**'lık (long) **miktar bilgisi** içermektedir. Bu kayıtları inceleyerek her gruptaki toplam miktarı grup numaralarına göre sıralı olarak bulan bir Map/Reduce uygulamasını yazınız. 

**Açıklamalar:**

1. **8 byte**'lık (long) grup bilgisi ve **8 byte**'lık (long) miktar bilgisi **Big Endian** biçimindedir.
2. Girdi verisinde kayıtlar arasında **newline** yada **boşluk** yoktur.
2. Çıktıda her satırda grup numarası ve bu grup numarasına ait toplam miktar bilgisi olacaktır. 
3. Çıktıdaki grup numarası ve miktar bilgisi arasında bir tane **boşluk** olacaktır. 
4. Satırlar grup numaralarına göre **küçükten büyüğe** göre **artan** sırada olacaktır.
5. Örnek girdi verisi `s3n://t2-hackathon-question-sampledata/2/input.txt` dosyasında, çıktı ise `s3n://t2-hackathon-sampleanswers/2/output.txt` dosyasında mevcuttur.

**İpucu:**

Verileri okumak için Hadoop'a özel kendi `InputFormat` ve `RecordReader` gerçekleştirimlerinizi yazmanız gerekmektedir.

**Örnek çıktı:**

~~~
0 492304
1 497372
2 486379
...
~~~

Soru-3 [10 Puan]
-----------
`s3n://t2-hackathon-question-data/3` dizini altında bulunan tüm tweetler incelenerek **unique** kullanıcı sayısını bulan bir Map/Reduce uygulamasını yazınız. 

**Açıklamalar:**

1. Kullanıcıyı unique yapan bilgi tweet datasındaki `user` alanının içindeki `id` değeridir. 
2. Çıktıda tek bir satırda unique kullanıcı sayısı değeri olacaktır.
3. Örnek girdi verisi `s3n://t2-hackathon-question-sampledata/3` dizininde, çıktı ise `s3n://t2-hackathon-sampleanswers/3/output.txt` dosyasında mevcuttur.

**Örnek çıktı:**

~~~
64098
~~~

Soru-4 [10 Puan]
-----------
`s3n://t2-hackathon-question-data/4` dizini altında bulunan tüm tweetler incelenerek profil bilgisi parametre geçilen dilde olan tweet sayısını bulan bir Map/Reduce uygulamasını yazınız. 

**Açıklamalar:**

1. Dil bilgisi tweet datasındaki `user` alanının içindeki `lang` değeridir. 
2. 1. parametrenin girdi verisinin dizini, 2. parametrenin çıktı verisinin dizini olduğunu hatırlarsak dil parametresi 3. parametredir. 
3. Sonuçlar karşılaştırılırken dil parametresi `tr` (**case-insensitive**) olarak geçilmiş olan sonuçlar ile karşılaştırılacaktır. Fakat aynı zaman da sonucun doğru olması halinde tam puan alabilmek için kod incelemesi de yapılacaktır.
4. Çıktıda tek satır bulunacaktır ve bu satırda sadece belirtilen dildeki tweet sayısı belirtilecektir. 
5. Örnek girdi verisi `s3n://t2-hackathon-question-sampledata/4` dizininde, çıktı ise `s3n://t2-hackathon-sampleanswers/4/output.txt` dosyasında mevcuttur.

**Örnek çıktı:**

~~~
29634
~~~

Soru-5 [10 Puan]
-----------
`s3n://t2-hackathon-question-data/5` dizini altında bulunan tüm tweetler incelenerek **en büyük** ve **en küçük** kullanıcı **id**'sini bulan bir Map/Reduce uygulamasını yazınız. 

**Açıklamalar:**

1. Kullanıcı **id**'si tweet datasındaki `user` alanının içindeki `id` değeridir. 
2. Çıktıda iki satır bulunacaktır. 1. satırda `MIN <min_id_value>`, 2. satırda `MAX <max_id_value>` değeri olacaktır.
3. Çıktıdaki `MIN <min_id_value>` ve `MAX <max_id_value>` satırlarındaki iki değer arasında sadece bir tane **boşluk** olacaktır.  
4. Örnek girdi verisi `s3n://t2-hackathon-question-sampledata/5` dizininde, çıktı ise `s3n://t2-hackathon-sampleanswers/5/output.txt` dosyasında mevcuttur.

**Örnek çıktı:**

~~~
MIN 4620
MAX 2322647593
~~~

Soru-6 [15 Puan]
-----------
`s3n://t2-hackathon-question-data/6`  dizini altında bulunan tüm tweetler incelenerek profil bilgisi `tr` (**case-insensitive**) dilinde olan kullanıcılardan popülarite skoru en yüksek olan kullanıcı ve onun skorunu bulan bir Map/Reduce uygulamasını yazınız. 

**Açıklamalar:**

1. Dil bilgisi tweet datasındaki `user` alanının içindeki `lang` değeridir. 
2. Takipçi sayısı bilgisi tweet datasındaki `user` alanının içindeki `followers_count` değeridir. 
3. Atılan tweet sayısı bilgisi tweet datasındaki `user` alanının içindeki `statuses_count` değeridir. 
4. Tweet içinde bahsedilen (mention'lanan) kullanıcılar tweet datasındaki `entities` alanının içindeki `user_mentions` alanındaki kullanıcılardır. 
5. Skor tweet bazında o tweet atılma anındaki tweeti atan kullanıcı bilgileri kullanılarak `<followers_count> * <statuses_count>` olarak hesaplanacak ve kullanıcının skoru kendisinden bahsedilen (mention'landığı) tweetlerin skorları toplanarak hesaplanacaktır. Örnegin **A** kullanıcısı attığı bir tweet'de **B** ve **C** kullanıcısından bahsetmiş ise (mention'lamış ise) ve **A** kullanıcısının tweet atılma anında follower sayısı 100 ve tweet sayısı da 1000 ise, o tweet için **B** ve **C** kullanıcılarına `100 * 1000` = **100000** puan skor olarak eklenecektir. Tweet'i atan A kullanıcısına ise herhangi bir puan eklenmeyecektir.
6. Eğer iki kullanıcının popülarite skoru aynı ise, `id` değeri büyük olan kullanıcının skoru daha büyük olarak kabul edilecektir. 
7. Çıktıda tek satır bulunacaktır ve popülarite skoru en yüksek olan kullanıcı id'si ve popülarite skoru aralarında bir tane **boşluk** olacak şekilde belirtilecektir. 
8. Örnek girdi verisi `s3n://t2-hackathon-question-sampledata/6` dizininde, çıktı ise `s3n://t2-hackathon-sampleanswers/6/output.txt` dosyasında mevcuttur.

**Örnek çıktı:**

~~~
252532055 46028061540
~~~

Soru-7 [15 Puan]
-----------
`s3n://t2-hackathon-question-data/7` dizini altında bulunan tüm tweetler incelenerek değer skoru **en büyük** ve **en küçük** olan kullanıcı **id**'sini ve değer skorunu bulan bir Map/Reduce uygulamasını yazınız. 

**Açıklamalar:**

1. Kullanıcı **id**'si tweet datasındaki `user` alanının içindeki `id` değeridir. 
2. Bir kullanıcı tweet attığı zaman o kullanıcının değer skoru **1 eksilir**. Bir tweet içinde bir kullanıcıdan bahsedildiğinde ise o kullanıcının değer skoru **1 arttırılır**. Örnegin **A** kullanıcısı attığı bir tweet'de **B** ve **C** kullanıcısından bahsetmiş ise (mention'lamış ise), o tweet için **A** kullanıcısının değer skoru **-1** ile toplanır, **B** ve **C** kullanıcılarının ise değer skoru **+1** ile toplanır.
3. Minimum değer karşılaştırması yapılırken eğer iki kullanıcının değer skoru aynı ise, `id` değeri küçük olan kullanıcının skoru daha küçük olarak kabul edilecektir.
4. Maximum değer karşılaştırması yapılırken eğer iki kullanıcının değer skoru aynı ise, `id` değeri büyük olan kullanıcının skoru daha büyük olarak kabul edilecektir.
5. Çıktıda iki satır bulunacaktır. 1. satırda `MIN <min_id_value>`, 2. satırda `MAX <max_id_value>` değeri olacaktır.
6. Çıktıdaki `MIN <min_id_value>` ve `MAX <max_id_value>` satırlarındaki iki değer arasında sadece bir tane **boşluk** olacaktır.  
7. Örnek girdi verisi `s3n://t2-hackathon-question-sampledata/7` dizininde, çıktı ise `s3n://t2-hackathon-sampleanswers/7/output.txt` dosyasında mevcuttur.

**Örnek çıktı:**

~~~
MIN 497145453 -170
MAX 209708391 796
~~~

Soru-8 [10 Puan]
-----------
`s3n://t2-hackathon-question-data/8` dizini altında bulunan tüm tweetler incelenerek toplam tweet sayısını bulan bir Map/Reduce uygulamasını yazınız. 

**Açıklamalar:**

1. Çıktıda tek bir satırda toplam tweet sayısı değeri olacaktır.
2. Örnek girdi verisi `s3n://t2-hackathon-question-sampledata/8` dizininde, çıktı ise `s3n://t2-hackathon-sampleanswers/8/output.txt` dosyasında mevcuttur.

**Örnek çıktı:**

~~~
120000
~~~

Soru-9 [20 Puan]
-----------
`s3n://t2-hackathon-question-data/9` dizini altında bulunan tüm tweetler incelenerek **1. dereceden** ve **2. dereceden** toplam iletişime girdiği kullanıcı sayısı en yüksek olan kullanıcı **id**'sini ve iletişime girdiği kullanıcı sayısını bulan bir Map/Reduce uygulamasını yazınız. 

**Açıklamalar:**

1. Kullanıcı **id**'si tweet datasındaki `user` alanının içindeki `id` değeridir. 
2. Tweet içinde bahsedilen (mention'lanan) kullanıcılar tweet datasındaki `entities` alanının içindeki `user_mentions` alanındaki kullanıcılardır. 
3. **1. dereceden** iletişime girdiği kullanıcılar, kullanıcının tweetlerinde bahsettiği (mention'ladığı) kullanıcılardır. Örnegin **A** kullanıcısı attığı bir tweet'de **B** ve **C** kullanıcısından bahsetmiş ise (mention'lamış ise), **A** kullanıcısı **B** ve **C** kullanıcıları ile iletişime girmiştir ve dolayısıyla iletişime giren kullanıcı çiftleri `(A->B), (A->C), (B->A), (C->A)` şeklindedir. Burada dikkat edilmesi gereken nokta iletişimin çift yönlü olduğu ve **B** ve **C** kullanıcısının da **A** ile iletişime girdiğidir.
4. **2. dereceden** iletişime girdiği kullanıcılar, **1. dereceden** iletişime girdiği kullanıcıların da doğrudan iletişime girdiği kullanıcılardır. Örneğin **A** kullanıcısı **B** ve **C** ile iletişimde, **B** kullanıcısı **C** ve **D** kullanıcısı ile iletişimde, **C** kullanıcısı da **D** ve **E** kullanıcısı ile iletişimde ise **A** kullanıcısı **2. dereceden** **D** ve **E** kullanıcısı ile iletişimdedir. Bunun tersini düşünürsek **D** ve **E** kullanıcısı da **A** kullanıcısı ile 2. dereceden iletişimdedir.
5. **3**. ve **4.** maddedeki örnekleri dikkate alırsak **A** kullanısının **1.** ve **2.** dereceden iletişime girdiği kullanıcılar **B, C, D ve E** kullanıcılarıdır ve dolayısıyla iletişime girdiği kullanıcı sayısı **4**'tür.
6. Eğer iki kullanıcının iletişime girdiği kullanıcı skoru aynı ise, `id` değeri büyük olan kullanıcının skoru daha büyük olarak kabul edilecektir. 
7. Çıktıda tek satır bulunacaktır ve iletişimde olduğu kişi sayısı en yüksek olan kullanıcı id'si ve iletişimde olduğu kişi sayısı aralarında bir tane **boşluk** olacak şekilde belirtilecektir. 
8. Örnek girdi verisi `s3n://t2-hackathon-question-sampledata/9` dizininde, çıktı ise `s3n://t2-hackathon-sampleanswers/9/output.txt` dosyasında mevcuttur.

**Örnek çıktı:**

~~~
1046019948 387
~~~
