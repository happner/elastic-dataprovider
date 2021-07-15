const MongoToElastic = require('../lib/mongo-to-elastic');
describe('func', function() {
  this.timeout(5000);
  const expect = require('expect.js');

  it('ensures that spaces in the query path are correctly escaped', () => {
    expect(
      MongoToElastic.convertCriteria({
        path: '/TEST/PATH/TEST LEAF/*'
      })
    ).to.eql('( path:/\\/TEST\\/PATH\\/TEST LEAF\\/.*/ ) ');
    expect(
      MongoToElastic.convertCriteria({
        path: '/TEST/PATH/TEST LEAF/1/2/3'
      })
    ).to.eql('( path:\\/TEST\\/PATH\\/TEST// LEAF\\/1\\/2\\/3 ) ');
    expect(
      MongoToElastic.convertCriteria({
        path: '/TEST/PATH/TEST LEAF/*',
        $and: [{ test: { $lte: 1 } }, { test: { $gte: 0 } }]
      })
    ).to.eql(
      '( path:/\\/TEST\\/PATH\\/TEST LEAF\\/.*/ )  AND ( ( ( ( test:<=1 )  )  AND ( ( test:>=0 )  )  ) ) '
    );
    expect(
      MongoToElastic.convertCriteria({
        path: '/TEST/PATH/TEST LEAF/1/2/3',
        $and: [{ test: { $lte: 1 } }, { test: { $gte: 0 } }]
      })
    ).to.eql(
      '( path:\\/TEST\\/PATH\\/TEST// LEAF\\/1\\/2\\/3 )  AND ( ( ( ( test:<=1 )  )  AND ( ( test:>=0 )  )  ) ) '
    );
  });
});
