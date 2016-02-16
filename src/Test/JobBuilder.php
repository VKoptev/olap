<?php

namespace Test;


/**
 * Class JobBuilder
 * @package Test
 * @method JobBuilder setData($data)
 */
class JobBuilder extends \JobQueue\JobBuilder {

    const TYPE = 100;

    protected function type() {

        return self::TYPE;
    }
}