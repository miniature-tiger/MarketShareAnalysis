// download data
const Json2csvParser = require('json2csv').Parser;
const fs = require('fs');
var path = require('path');
var JSONStream = require('JSONStream');
es = require('event-stream');

// ------------ INPUTS ---------------
const datesArray = {
    June: {monthFirst: '2018-06-01', monthLastClosed: '2018-06-23', pathNamedepth0: 'JuneFullSummApp.json', pathNamedepthAll: 'JuneFullSummAppAllDepth.json'} ,
    July: {monthFirst: '2018-07-01', monthLastClosed: '2018-07-24', pathNamedepth0: 'JulyFullSummApp.json', pathNamedepthAll: 'JulyFullSummAppAllDepth.json'} ,
              }

const analysisMonth = 'July';
const dataDepth = 'depth0'; // depth0 or depthAll
const platform = 'All'; // platform_name or All

// -----------------------------------

const postsDate = datesArray[analysisMonth].monthFirst;
const paymentsDate = datesArray[analysisMonth].monthLastClosed;

let counter = 0;
let myData = '';
let cleanData = '';
let summarisedData = [];
let summaryByDateAuthor = [];
let summaryByDate = [];
let authorData = {};
let otherData = [];
let useCount = [];

if (dataDepth == 'depth0') {
    myFile = path.join(__dirname, datesArray[analysisMonth].pathNamedepth0);
} else if (dataDepth == 'depthAll') {
    myFile = path.join(__dirname, datesArray[analysisMonth].pathNamedepthAll);
} else {
    console.log('error in file path');
}

console.log('-------------------- START --------------------');
console.log('analysisMonth:', analysisMonth, ', dataDepth:', dataDepth)
console.log(myFile);


var rStream = fs.createReadStream(myFile, {encoding: 'utf8'});
var parser = JSONStream.parse('*');

var getStream = function() {
    return rStream.pipe(parser);
}

getStream()
    .pipe(es.mapSync(function (data) {
        cleanData = data;

      }));

rStream.on('end', function() {
    console.log('finished reading with ' + counter + ' chunks');
    if(platform == 'All') {
        dataSummarise();
        authorSummarise();
        outputClean();
        dataExport(summarisedData.slice(0), otherData.slice(0), 'dataExportFull' + '_' + analysisMonth + '_' + dataDepth);
        outputReduce();
        dataExport(summarisedData.slice(0), otherData.slice(0), 'dataExportTop50' + '_' + analysisMonth + '_' + dataDepth);
        extraExport(useCount, 'otherStatsExport' + '_' + analysisMonth + '_' + dataDepth);
    } else {
        dateSummarise(platform);
        dateExport(summaryByDate, platform + '_' + analysisMonth + '_' + dataDepth);
    }
    console.log('-------------------- END --------------------');
});

function dataSummarise() {
    console.log('------------ SUMMARISING DATA ---------------');
    console.log('posts count and payout amounts');
    let dateErrorCount = 0;
    for (var i = 0; i < cleanData.length; i+=1) {
        if(i % 500000 == 0) {
            console.log('processed to record ' + i + ' of ' + cleanData.length + '.');
        }

        if (summarisedData.length == 0) {
            if (cleanData[i].Date >= postsDate && cleanData[i].Date <= paymentsDate) {
                summarisedData.push({Application: cleanData[i].SummarisedApp, Posts: 1, Authors: 0, AuthorPayout: cleanData[i].TotalPayoutValue, CuratorPayout: cleanData[i].CuratorPayoutValue, PendingPayout: cleanData[i].PendingPayoutValue});
            } else if (cleanData[i].Date > paymentsDate) {
                summarisedData.push({Application: cleanData[i].SummarisedApp, Posts: 1, Authors: 0, AuthorPayout: 0, CuratorPayout: 0, PendingPayout:0});
            } else if (cleanData[i].Date < postsDate) {
                summarisedData.push({Application: cleanData[i].SummarisedApp, Posts: 0, Authors: 0, AuthorPayout: cleanData[i].TotalPayoutValue, CuratorPayout: cleanData[i].CuratorPayoutValue, PendingPayout: cleanData[i].PendingPayoutValue});
            } else {
                dateErrorCount += 1;
            }

        } else {
            let arrayPosition = summarisedData.findIndex(fI => fI.Application == cleanData[i].SummarisedApp);

            if (arrayPosition == -1) {
                if (cleanData[i].Date >= postsDate && cleanData[i].Date <= paymentsDate) {
                    summarisedData.push({Application: cleanData[i].SummarisedApp, Posts: 1, Authors: 0, AuthorPayout: cleanData[i].TotalPayoutValue, CuratorPayout: cleanData[i].CuratorPayoutValue, PendingPayout: cleanData[i].PendingPayoutValue});
                } else if (cleanData[i].Date > paymentsDate) {
                    summarisedData.push({Application: cleanData[i].SummarisedApp, Posts: 1, Authors: 0, AuthorPayout: 0, CuratorPayout: 0, PendingPayout:0});
                } else if (cleanData[i].Date < postsDate) {
                    summarisedData.push({Application: cleanData[i].SummarisedApp, Posts: 0, Authors: 0, AuthorPayout: cleanData[i].TotalPayoutValue, CuratorPayout: cleanData[i].CuratorPayoutValue, PendingPayout: cleanData[i].PendingPayoutValue});
                } else {
                    dateErrorCount += 1;
                }
            } else {
                if (cleanData[i].Date >= postsDate && cleanData[i].Date <= paymentsDate) {
                    summarisedData[arrayPosition].Posts += 1;
                    summarisedData[arrayPosition].AuthorPayout += cleanData[i].TotalPayoutValue;
                    summarisedData[arrayPosition].CuratorPayout += cleanData[i].CuratorPayoutValue;
                    summarisedData[arrayPosition].PendingPayout += cleanData[i].PendingPayoutValue;
                } else if (cleanData[i].Date > paymentsDate) {
                    summarisedData[arrayPosition].Posts += 1;
                } else if (cleanData[i].Date < postsDate) {
                    summarisedData[arrayPosition].AuthorPayout += cleanData[i].TotalPayoutValue;
                    summarisedData[arrayPosition].CuratorPayout += cleanData[i].CuratorPayoutValue;
                    summarisedData[arrayPosition].PendingPayout += cleanData[i].PendingPayoutValue;
                } else {
                    dateErrorCount += 1;
                }
            }
        }
    }

    console.log('dateErrorCount', dateErrorCount);
}

function authorSummarise() {
console.log('author count and application per author count')
    let authorName = '';
    for (var i = 0; i < cleanData.length; i+=1) {
        if(i % 500000 == 0) {
            console.log('processed to record ' + i + ' of ' + cleanData.length + '.');
        }
        if (cleanData[i].Date >= postsDate) {
            authorName = cleanData[i].Author;

            if (authorData.hasOwnProperty(authorName)) {
                if(authorData[authorName].indexOf(cleanData[i].SummarisedApp) == -1) {
                    authorData[authorName].push(cleanData[i].SummarisedApp);
                }
            } else {
                authorData[authorName] = [cleanData[i].SummarisedApp];
            }
        }
    }

    let appListLength = 0;

    for (var author in authorData) {
        appListLength = authorData[author].length;

        if (useCount[appListLength-1] === undefined) {

            countObj = {appsUsed: appListLength, count: 1};

            useCount[appListLength-1] = countObj;
        } else {
            useCount[appListLength-1].count = useCount[appListLength-1].count + 1;
        }
    }

    for (var author in authorData) {
        for (var j = 0; j < authorData[author].length; j++) {
            let arrayPosition = summarisedData.findIndex(fI => fI.Application == authorData[author][j]);
            summarisedData[arrayPosition].Authors += 1;
        }
    }
}


function outputClean() {
    console.log('------------ CLEANING AND RANKING OUTPUT ---------------');
    otherData = [{Application: 'other', Posts: 0, Authors: 0, AuthorPayout: 0, CuratorPayout: 0, PendingPayout: 0, TotalPayout: 0}];
    for (var i = summarisedData.length - 1; i > -1 ; i-=1) {
        summarisedData[i].AuthorPayout = Number(summarisedData[i].AuthorPayout.toFixed(2));
        summarisedData[i].CuratorPayout = Number(summarisedData[i].CuratorPayout.toFixed(2));
        summarisedData[i].PendingPayout = Number(summarisedData[i].PendingPayout.toFixed(2));
        summarisedData[i].TotalPayout = Number((summarisedData[i].AuthorPayout + summarisedData[i].CuratorPayout + summarisedData[i].PendingPayout).toFixed(2));

        if(summarisedData[i].Application == '' || summarisedData[i].Application == ' ' || summarisedData[i].Application == null || summarisedData[i].Application == "null") {
            otherDataAdd(i);
        }
    }

    ranking('Authors', 'Desc');
    ranking('Posts', 'Desc' );
    ranking('TotalPayout', 'Desc');

    for (var l = 0; l < summarisedData.length; l+=1) {
        summarisedData[l].Overall = (summarisedData[l].AuthorsRank + summarisedData[l].TotalPayoutRank)/2
    }

    ranking('Overall', 'Asc');

    function ranking(rankItem, rankOrder) {
        let rankingArray = [];
        let sortedArray = [];

        for (var j = 0; j < summarisedData.length; j+=1) {
            rankingArray.push(summarisedData[j][rankItem]);
        }
        if (rankOrder == 'Desc') {
            sortedArray = rankingArray.slice().sort(function(a,b){return b-a});
        } else {
            sortedArray = rankingArray.slice().sort(function(a,b){return a-b});
        }

        let finalRanks = rankingArray.slice().map(function(c){return sortedArray.indexOf(c)+1});

        let outputItem = rankItem + 'Rank'
        for (var k = 0; k < summarisedData.length; k+=1) {
            //console.log(k, finalRanks[k]);
            summarisedData[k][outputItem] = finalRanks[k];
        }

        return finalRanks;

    }

}

function otherDataAdd(locali) {
    otherData[0].Posts += summarisedData[locali].Posts;
    otherData[0].Authors += summarisedData[locali].Authors;
    otherData[0].AuthorPayout += summarisedData[locali].AuthorPayout;
    otherData[0].CuratorPayout += summarisedData[locali].CuratorPayout;
    otherData[0].PendingPayout += summarisedData[locali].PendingPayout;
    otherData[0].TotalPayout += summarisedData[locali].TotalPayout;
    summarisedData.splice(locali,1);
}


function dataExport(localSummarisedData, localOtherData, localFileName) {
    console.log('--------- Exporting Data : ' + localFileName + '---------')
    let exportData = localSummarisedData;
    exportData.push(localOtherData[0]);
    const fields = ['Application', 'Authors', 'AuthorsRank', 'Posts', 'PostsRank', 'AuthorPayout', 'CuratorPayout', 'PendingPayout', 'TotalPayout', 'TotalPayoutRank', 'Overall', 'OverallRank'];
    let json2csvParser = new Json2csvParser({fields});
    let csvExport = json2csvParser.parse(exportData);
    fs.writeFile(localFileName + '.csv', csvExport, function (err) {
        if (err) throw err;
    });
}

function extraExport(localData, localFileName) {
    console.log('--------- Exporting Data : ' + localFileName + '---------')
    let exportData = localData;
    const fields = ['appsUsed', 'count'];
    let json2csvParser = new Json2csvParser({fields});
    let csvExport = json2csvParser.parse(exportData);
    fs.writeFile(localFileName + '.csv', csvExport, function (err) {
        if (err) throw err;
    });
}

function outputReduce() {
    for (var i = summarisedData.length - 1; i > -1 ; i-=1) {
        if(summarisedData[i].OverallRank > 50) {
            otherDataAdd(i);
        } else {
            //console.log('not removing ' + summarisedData[i].Application);
        }
    }
    otherData[0].AuthorPayout = Number(otherData[0].AuthorPayout.toFixed(2));
    otherData[0].CuratorPayout = Number(otherData[0].CuratorPayout.toFixed(2));
    otherData[0].PendingPayout = Number(otherData[0].PendingPayout.toFixed(2));
    otherData[0].TotalPayout = Number(otherData[0].TotalPayout.toFixed(2));

}


function dateSummarise(localPlatform) {
    console.log('------------ DATE ANALYSIS ---------------');
    console.log('looking at ' + platform);

    for (var i = 0; i < cleanData.length; i+=1) {
        if(i % 500000 == 0) {
            console.log('processed to record ' + i + ' of ' + cleanData.length + '.');
        }

        if(cleanData[i].SummarisedApp == platform) {
            if (summaryByDateAuthor.length == 0) {
                summaryByDateAuthor.push({createdDate: cleanData[i].Date, Author: cleanData[i].Author, Posts: 1, AuthorPayout: cleanData[i].TotalPayoutValue, CuratorPayout: cleanData[i].CuratorPayoutValue, PendingPayout: cleanData[i].PendingPayoutValue});
            }

            let arrayPosition = summaryByDateAuthor.findIndex(fI => fI.createdDate == cleanData[i].Date && fI.Author == cleanData[i].Author);
            if (arrayPosition == -1) {
                summaryByDateAuthor.push({createdDate: cleanData[i].Date, Author: cleanData[i].Author, Posts: 1, AuthorPayout: cleanData[i].TotalPayoutValue, CuratorPayout: cleanData[i].CuratorPayoutValue, PendingPayout: cleanData[i].PendingPayoutValue});
            } else {
                summaryByDateAuthor[arrayPosition].Posts += 1;
                summaryByDateAuthor[arrayPosition].AuthorPayout += cleanData[i].TotalPayoutValue;
                summaryByDateAuthor[arrayPosition].CuratorPayout += cleanData[i].CuratorPayoutValue;
                summaryByDateAuthor[arrayPosition].PendingPayout += cleanData[i].PendingPayoutValue;
            }
        }
    }

    for (var j = 0; j < summaryByDateAuthor.length; j+=1) {
        if (summaryByDate.length == 0) {
            summaryByDate.push({createdDate: summaryByDateAuthor[j].createdDate, Authors: 1, Posts: summaryByDateAuthor[j].Posts, AuthorPayout: summaryByDateAuthor[j].AuthorPayout, CuratorPayout: summaryByDateAuthor[j].CuratorPayout, PendingPayout: summaryByDateAuthor[j].PendingPayout});
        }
        let arrayPosition = summaryByDate.findIndex(fI => fI.createdDate == summaryByDateAuthor[j].createdDate);
        if (arrayPosition == -1) {
            summaryByDate.push({createdDate: summaryByDateAuthor[j].createdDate, Authors: 1, Posts: summaryByDateAuthor[j].Posts, AuthorPayout: summaryByDateAuthor[j].AuthorPayout, CuratorPayout: summaryByDateAuthor[j].CuratorPayout, PendingPayout: summaryByDateAuthor[j].PendingPayout});
        } else {
            summaryByDate[arrayPosition].Posts += summaryByDateAuthor[j].Posts;
            summaryByDate[arrayPosition].Authors += 1;
            summaryByDate[arrayPosition].AuthorPayout += summaryByDateAuthor[j].AuthorPayout;
            summaryByDate[arrayPosition].CuratorPayout += summaryByDateAuthor[j].CuratorPayout;
            summaryByDate[arrayPosition].PendingPayout += summaryByDateAuthor[j].PendingPayout;
        }

    }

    for (var k = 0; k < summaryByDate.length; k+=1) {
        summaryByDate[k].AuthorPayout = Number(summaryByDate[k].AuthorPayout.toFixed(2));
        summaryByDate[k].CuratorPayout = Number(summaryByDate[k].CuratorPayout.toFixed(2));
        summaryByDate[k].PendingPayout = Number(summaryByDate[k].PendingPayout.toFixed(2));
        summaryByDate[k].TotalPayout = Number((summaryByDate[k].AuthorPayout + summaryByDate[k].CuratorPayout + summaryByDate[k].PendingPayout).toFixed(2));
    }
}

function dateExport(localData, localFileName) {
    console.log('--------- Exporting Date Data : ' + localFileName + '---------')
    let exportData = localData;
    const fields = ['createdDate', 'Authors', 'Posts', 'AuthorPayout', 'CuratorPayout', 'PendingPayout', 'TotalPayout'];
    let json2csvParser = new Json2csvParser({fields});
    let csvExport = json2csvParser.parse(exportData);
    fs.writeFile(localFileName + '.csv', csvExport, function (err) {
        if (err) throw err;
    });
}
