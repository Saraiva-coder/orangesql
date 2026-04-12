#include "utils.h"

namespace orangesql {

thread_local std::mt19937_64 Random::gen(std::random_device{}());

}