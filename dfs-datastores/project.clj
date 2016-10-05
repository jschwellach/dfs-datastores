(def ROOT-DIR (subs *file* 0 (- (count *file*) (count "project.clj"))))
(def VERSION (-> ROOT-DIR (str "/../VERSION") slurp))

(defproject com.backtype/dfs-datastores VERSION
  :description "Dead-simple vertical partitioning, compression, appends, and consolidation of data on a distributed filesystem."
  :url "https://github.com/nathanmarz/dfs-datastores"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.slf4j/slf4j-api "1.6.6"]
                 [jvyaml/jvyaml "1.0.0"]
                 [com.google.guava/guava "13.0"]
                 [junit/junit "4.12"]]
  :deploy-repositories {"releases" {:url "https://oss.sonatype.org/service/local/staging/deploy/maven2"
                                    :creds :gpg}
                        "snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots"
                                     :creds :gpg}}
  :scm {:connection "scm:git:git://github.com/nathanmarz/dfs-datastores.git"
        :developerConnection "scm:git:ssh://git@github.com/nathanmarz/dfs-datastores.git"
        :url "https://github.com/nathanmarz/dfs-datastores"
        :dir ".."}
   :plugins [[no-man-is-an-island/lein-eclipse "2.0.0"]]
   :pom-addition [:developers
                 [:developer
                  [:name "Nathan Marz"]
                  [:url "http://twitter.com/nathanmarz"]]
                 [:developer
                  [:name "Soren Macbeth"]
                  [:url "http://twitter.com/sorenmacbeth"]]
                 [:developer
                  [:name "Sam Ritchie"]
                  [:url "http://twitter.com/sritchie"]]]
  :javac-options ["-source" "1.8" "-target" "1.8"]
  :java-source-paths ["src/main/java" "src/test/java"]
  :junit ["src/test/java"]
  :profiles {:dev
             {:dependencies [[org.slf4j/slf4j-log4j12 "1.6.6"]]
              :plugins [[lein-junit "1.1.8"]]}
             :provided
             {:dependencies [[org.apache.hadoop/hadoop-common "2.7.3"]
                             [org.apache.spark/spark-core_2.11 "2.0.0"]]}
             :test
             {:dependencies [[junit/junit "4.12"]]}
             }
  :classifiers {:javadoc {:java-source-paths ^:replace []
                          :source-paths ^:replace []
                          :resource-paths ^:replace []}
                :sources {:java-source-paths ^:replace []
                          :resource-paths ^:replace []}}
  
  :pom-plugins [[org.apache.maven.plugins/maven-compiler-plugin "3.5.1"
                 ;; this section is optional, values have the same syntax as pom-addition
                 {:configuration ([source "1.8"]
                                   [target "1.8"]
                                   [encoding "UTF-8"])
                  }
                ]]
  
  )
