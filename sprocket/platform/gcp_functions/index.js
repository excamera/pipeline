/**
 *  *  * Responds to any HTTP request that can provide a "message" field in the body.
 *   *   *
 *    *    * @param {!Object} req Cloud Function request context.
 *     *     * @param {!Object} res Cloud Function response context.
 *      *      */
exports.entry = (req, res) => {
    req.body.start_ts = new Date().getTime()/1000;
    var child_process = require("child_process");

    console.log('to run python entry.py');
    var buf = require('child_process').execSync('export LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH '+
                                                'AWS_ACCESS_KEY_ID='+req.body.akid+' AWS_SECRET_ACCESS_KEY='+req.body.secret+';'+
                                                'python entry.py '+'\''+JSON.stringify(req.body)+'\'');
    console.log(buf.toString());
    console.log('finish running python entry.py');
    res.status(200).send('Success');
};
